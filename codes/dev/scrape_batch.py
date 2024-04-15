import utils
import main
import time
from datetime import datetime
import pandas as pd
import sqlalchemy
import mysql.connector
import resource


def get_csv_timetables(task_num, path = '/shared/share_rent/craigslist/reference_files/timetable.csv'):
    
    with open(path, 'r') as file:
        THIS_TIME_df = pd.read_csv(file)
    time.sleep(1)
    THIS_TIME_df = THIS_TIME_df[THIS_TIME_df['task_num'] == task_num]
    ### gather sub_region information online and generate region_df
    region_df = main.aggregate_region_df()
    region_df_toDo =pd.concat([region_df[['region', 'sub_region']],THIS_TIME_df[['region', 'sub_region']]]).drop_duplicates(keep=False)
    time.sleep(1)
    return region_df, region_df_toDo

if __name__ == "__main__":
    # parsing command line arguments as input for tasknum and date
    # args = utils.parseArguments()
    # if args.date:
    #     TODAY = datetime.strptime(args.date, "%Y-%m-%d").date()
    # else:
    #     TODAY = datetime.today().date()
    # # credentials of the database for connecting
    
    # Increase soft and hard limits for number of open file descriptors
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (8192, hard))

    with open('/shared/share_rent/craigslist/credentials/credentials.csv', 'r') as f:
        creds = pd.read_csv(f)
    time.sleep(1)

    creds = creds.loc[creds['on_grid'] == True]
    task_num = 71
    d = "2024-04-08"
    TODAY = datetime.strptime(d, "%Y-%m-%d").date()
    # region_df_toDo_len = 99
    # while region_df_toDo_len != 0:
    region_df, region_df_toDo = get_csv_timetables(task_num)
    # region_df_toDo_len = len(region_df_toDo)
    # print("Remaining number of regions is {}".format(region_df_toDo_len))
    ## start scraping!
    # try:
    main.scrape(region_df_toDo, task_num, TODAY)
    # except:
    #     continue
        # after scraping, now work on pre-processing before upload

    df = utils.create_df()
    df = utils.pre_process(df)
    df = utils.assign_geocode(df)
    df = utils.assign_zipcode(df)
    most_frequent_dates = df['scraped_week'].value_counts().index.tolist()
    print(most_frequent_dates)
    df = df.loc[df['scraped_week'] == most_frequent_dates[0]]

    # Split by CA-only vs Top 100 MSA
    df = pd.merge(df, region_df, how = "left", on = ['region', 'sub_region'])
    df_top100 = df.loc[(df['region_merge'] == 'right_only') | (df['region_merge'] == 'both')]
    df_CA = df.loc[(df['region_merge'] == 'left_only') | (df['region_merge'] == 'both')]

    # Sorting
    df_top100 = df_top100.sort_values(by=['region', 'sub_region', 'p_id'])
    df_CA = df_CA.sort_values(by=['region', 'sub_region', 'p_id'])


    # # now upload to sql database
    ENGINE_top100 = sqlalchemy.create_engine(url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(creds.iloc[0]['user_name'], creds.iloc[0]['password'], creds.iloc[0]['host'], str(creds.iloc[0]['port']), "craigslist_full"))
    with ENGINE_top100.connect() as con:
        df_top100[['p_id','geoid','zip','price','beds', 'baths', 'sqft', 'map', 'lat', 'long','scraped_week','posted_time', 'last_updated_time','region', 'sub_region']].to_sql(con=con,  index = False, name='essential',if_exists='append', chunksize = 5000, method = 'multi')
        df_top100[['p_id','geoid','zip','title','price_o','beds_o', 'sqft_o', 'tag1', 'tag2', 'tag3','url', 'descr','scraped_week','posted_time', 'last_updated_time','last_updated_time_o','region', 'sub_region']].to_sql(con=con,  index = False, name='other',if_exists='append', chunksize = 5000, method = 'multi')


    ENGINE_CA = sqlalchemy.create_engine(url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(creds.iloc[0]['user_name'], creds.iloc[0]['password'], creds.iloc[0]['host'], str(creds.iloc[0]['port']), "craigslist_CA"))
    with ENGINE_CA.connect() as con:
        df_CA[['p_id','geoid','zip','price','beds', 'baths', 'sqft', 'map', 'lat', 'long','scraped_week','posted_time', 'last_updated_time','region', 'sub_region']].to_sql(con=con,  index = False, name='essential',if_exists='append', chunksize = 5000, method = 'multi')
        df_CA[['p_id','geoid','zip','title','price_o','beds_o', 'sqft_o', 'tag1', 'tag2', 'tag3','url', 'descr','scraped_week','posted_time', 'last_updated_time','last_updated_time_o','region', 'sub_region']].to_sql(con=con,  index = False, name='other',if_exists='append', chunksize = 5000, method = 'multi')
