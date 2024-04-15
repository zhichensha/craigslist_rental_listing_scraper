import sys
import os
import glob
import argparse
import pandas as pd
import numpy as np
import re
from censusgeocode import CensusGeocode
from concurrent.futures import ThreadPoolExecutor
from tqdm.notebook import tqdm
from pandarallel import pandarallel
import geopandas as gpd
from shapely.geometry import Point, Polygon

### Preprocessing functions for cleaning up and adding/adjusting variables

def create_df(path = "/shared/share_rent/craigslist/local_data"):
    """create a single dataframe containing all scraped data from craigslist that is stored in the path locally (and temporarily)
    Args:
        path: a string variable indicating the directory that stores all the data; optional as default value is given
    Returns:
        df: a pd.DataFramec ontaining all scraped data for the week
    """
    df = pd.DataFrame()
    count = 0
    for folder in os.listdir(path): # loop through all region folders in the path
        dir_path = "{}/{}/".format(path, folder)
        for file_name in glob.glob(dir_path+'*.csv'):
            df_each = pd.read_csv(file_name) # read each csv file in the region folder
            print(file_name)
            df = pd.concat([df, df_each], ignore_index = True) # concat dfs iteratively
            count += 1
    return df

def extract_info_from_list(list_of_strings):
    if list_of_strings is None or list_of_strings is np.nan:
        return None, None, None 
    beds = baths = sqft = None
    for item in list_of_strings:
        if 'BR' in item and 'Ba' in item:
            beds_baths_match = re.search(r'(\d+)BR.*?(\d+)Ba', item)
            if beds_baths_match:
                beds = int(beds_baths_match.group(1))
                baths = int(beds_baths_match.group(2))
        elif 'ft2' in item:
            sqft_match = re.search(r'(\d+)ft2', item)
            if sqft_match:
                sqft = int(sqft_match.group(1))
    return beds, baths, sqft

def pre_process(df):
    """takes the dataframe generated from create_df and clean-up: remove variables we don't want and extract variables w
    from the existing variables
    Args:
        df: a pd.DataFrame ocntaining all scraped data for the week
    Returns:
        df: a cleaned pd.DataFrame containing all scraped data
    """
    # dropping variables and remove listings with empty information
    df = df.drop(['ip_proxy', 'scraped'], axis = 1)
    df = df[df['descr'] != 'nan']
    df['descr'] = df['descr'].astype(str)

    # adjusting the format of the time scraped from craigslist to standard format(remove UTC offset and convert to US/Eastern timezone)
    df['last_updated_time_o'] = pd.to_datetime(df['last_updated_time_o'], errors='coerce')
    df['last_updated_time'] = pd.to_datetime(df['last_updated_time'], utc=True).dt.tz_convert('US/Eastern').dt.tz_localize(None)
    df['posted_time'] = pd.to_datetime(df['posted_time'], utc=True).dt.tz_convert('US/Eastern').dt.tz_localize(None)

    ## Split tag 1
    df['tag1_split'] = df['tag1'].str.replace(' ', '').str.split('\n')

    ## Gather beds, baths and sqft
    df[['beds', 'baths', 'sqft']] = df['tag1_split'].apply(lambda x: extract_info_from_list(x)).apply(pd.Series)
    df.drop(['tag1_split'], axis = 1, inplace = True)
    # # There are many instances with sharedBa or splitBa; we consider them as 0 bathrooms
    # df['floor_plan'] = df['floor_plan'].str.replace('shared', '0')
    # df['floor_plan'] = df['floor_plan'].str.replace('split', '0')

    # Split floor_plan into beds and baths, and also only keep numeric part of it
    # Remove BR and Ba, as well + sign in baths, then convert them to float
    # df['beds'] = pd.to_numeric(df['floor_plan'].str.split('/').str[0].str.replace('BR', '',regex=True).str.strip(), errors="coerce") ## updated 4/10/23 adding coerce
    # df['baths'] = df['floor_plan'].str.split('/').str[1].str.replace('Ba', '',regex=True)
    # df['baths'] = pd.to_numeric(df['baths'].str.replace('+', '', regex=False), errors="coerce")
    
    # Grab floor size information from tag1, and keep only observations with 'ft2' in it
    # as it may contain information on other such as availability
    # df['sqft'] = df['tag1'].str.split('\n').str[1]
    # df['sqft'] = df['sqft'].fillna('')
    # df['sqft'] = df.loc[df['sqft'].str.contains('ft2'), 'sqft']
    # df['sqft'] = df['sqft'].str.replace('ft2', '')
    # df['sqft'] = df['sqft'].astype(float)
    # df.drop(['floor_plan'], axis = 1, inplace = True)

    # Grab outside floor size information from floor_size_o, and keep only observations with 'ft2' in it
    df['sqft_o'] = df['floor_size_o'].str.strip().replace('ft2', '',regex=True)
    df['sqft_o'] = pd.to_numeric(df['sqft_o'], errors="coerce")
    df.drop(['floor_size_o'], axis = 1, inplace = True)
    return df


# Geocoding with censusgeocode
def geocode(row):
    index, lat, long = row
    ## select 2010 census track version
    cg = CensusGeocode(benchmark='Public_AR_Current', vintage='Census2010_Current')
    try:
        census = cg.coordinates(x=long, y=lat)['Census Tracts'][0]
        data = dict(geoid=census['GEOID'],
                    lat=lat, 
                    long=long)
    except Exception as e:
        data = dict(geoid=None,
                    lat=lat, 
                    long=long)
    return data

def assign_geocode(df):
    # get lattidue longitude data from df_essential, keeping only unique ones for API calls
    locations = df[['lat', 'long']].drop_duplicates().reset_index(drop=True)
    with ThreadPoolExecutor() as tpe:
        data = list(tqdm(tpe.map(geocode, locations.itertuples()), total=len(locations)))
    # retrieve geoid from data and merge them back to df_essential by lat-long
    locations = pd.DataFrame.from_records(data)
    df = df.merge(locations, on=['lat', 'long'], how = 'left')
    return df

### using shapefile downloaded from census https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2022&layergroup=ZIP+Code+Tabulation+Areas
### we can assign zip code using lattitude and longitude
def zipcode(row, data):
    point = Point(row['long'], row['lat'])
    try:
        row['zip'] = data.loc[point.within(data['geometry']), 'ZCTA5CE20'].values[0]
    except:
        pass
    return row
    
def assign_zipcode(df, path = "/shared/share_rent/craigslist/reference_files/shapefiles/zcta_files/tl_rd22_us_zcta520.shp"):
    pandarallel.initialize(progress_bar=True)
    data = gpd.read_file(path)
    locations = df[['lat', 'long']].drop_duplicates().reset_index(drop=True)
    locations['zip'] = None
    locations = locations.parallel_apply(lambda r: zipcode(r, data), axis = 1)
    df = df.merge(locations, on=['lat', 'long'], how = 'left')
    return df

### Parsing functions
def parseArguments():
    """takes input from shell/terminal/command line, parse them and pass them into the scripts as values for taskNum and scraped_week_date
    Args:
        None for this function
    Returns:
        args: a dictionary containing the input from shell/terminal/command line
    """
    # Create argument parser
    parser = argparse.ArgumentParser()

    # Positional mandatory arguments
    parser.add_argument("taskNum", help="Task # of this week's scrape task.", type=int)

    # Optional arguments
    parser.add_argument("-d", "--date", help="Scraped Date of this task. (YYYY-MM-DD)", required = False, type=str)

    # Print version
    parser.add_argument("--version", action="version", version='%(prog)s - Version 1.0')
    # Parse arguments
    args = parser.parse_args()
    return args
