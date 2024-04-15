import os
import re
import sys
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
import time
import random
import numpy as np
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def insert_scraped_time_csv(d, region, sub_region, task_num, path = '/shared/share_rent/craigslist/reference_files/timetable.csv'):
    new_row = {
    "id": None,
    "scraped_date": d,
    "region": region,
    "sub_region": sub_region,
    "task_num": task_num
}
    new_row_df = pd.DataFrame([new_row])
    # new_row_df.to_csv(path,  mode='a', header=False, index=False)
    with open(path, 'a') as f:
        new_row_df.to_csv(f, header=False, index=False)
        time.sleep(1)

def create_CA_region_df(path = 'https://geo.craigslist.org/iso/us/ca'):
    ### read in top100_msa and extracts region information from them
    ### gather sub_region information online and generate sub_region_df
    response = requests.get(path)
    soup = BeautifulSoup(response.content, 'html.parser')
    links = []
    for link in soup.find('ul', attrs={'class', 'height6 geo-site-list'}).find_all('a'):
        region_name = link.text.strip()
        link_url = link.get('href')
        links.append(link_url)
    df = pd.DataFrame()
    for url in links:
        region = re.search('https://(.*).craigslist.org', url).group(1)
        with requests.Session() as s:
            r = s.get(url)
            time.sleep(random.randint(1,2)/10)
            soup = BeautifulSoup(r.content, 'html.parser')
            try:
                sub_region_tags = soup.find('ul', attrs={'class':'sublinks'}).find_all('a')
                sub_region_tags = [tag['href'].replace('/', '') for tag in sub_region_tags]
            except:
                sub_region_tags = None
            if sub_region_tags is not None:
                for sub_region in sub_region_tags:
                    new_row = {'region': region, 'sub_region': sub_region}
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            elif sub_region_tags is None:
                new_row = {'region': region, 'sub_region': sub_region_tags}
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df = df.drop_duplicates().sort_values(by=['region', 'sub_region']).reset_index(drop=True)
    return df

def create_all_region_df(path = '/shared/share_rent/craigslist/reference_files/top100_msa.xlsx'):
    ### read in top100_msa and extracts region information from them
    ### gather sub_region information online and generate sub_region_df
    # top100_msa = pd.read_excel(path, sheet_name = "with_url")
    # top100_msa_urls = list(set(top100_msa['url']))
    with pd.ExcelFile(path) as xls:
        top100_msa = pd.read_excel(xls, sheet_name="with_url")
        top100_msa_urls = list(set(top100_msa['url']))
    time.sleep(2)
    df = pd.DataFrame()
    i = 0
    for url in top100_msa_urls:
        region = re.search('https://(.*).craigslist.org', url).group(1)
        with requests.Session() as s:
            r = s.get(url)
            time.sleep(random.randint(1,2)/10)
            soup = BeautifulSoup(r.content, 'html.parser')
            try:
                sub_region_tags = soup.find('ul', attrs={'class':'sublinks'}).find_all('a')
                sub_region_tags = [tag['href'].replace('/', '') for tag in sub_region_tags]
            except:
                sub_region_tags = None
            if sub_region_tags is not None:
                for sub_region in sub_region_tags:
                    new_row = {'region': region, 'sub_region': sub_region}
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            elif sub_region_tags is None:
                new_row = {'region': region, 'sub_region': sub_region_tags}
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df = df.drop_duplicates().sort_values(by=['region', 'sub_region']).reset_index(drop=True)
    return df


def aggregate_region_df():
    full_region_df = create_all_region_df()
    CA_region_df = create_CA_region_df()
    df = pd.merge(CA_region_df, full_region_df, on=['region', 'sub_region'], how='outer', indicator=True)
    df['is_CA'] = (df["_merge"] != "right_only")
    df.sort_values(by=['is_CA', 'region', 'sub_region'], ascending=[False, True, True])
    df.rename(columns={'_merge': 'region_merge'}, inplace=True)
    return df

def scrape_each_listing(row):
    """this function takes a row (which mainly contains url and proxy info), and try to scrape the information inside the page 
    Args:
        row: one row of data from df_craigslist_full, waiting to be scraped for the contained url and assigned ip_proxy
    Returns:
        d: a dictionary(can also be consider as one-line dataframe) that contains all the needed scraped information
    """
    ## REQ_HEADERS is used to provide metadata about the request, such as the type of data being sent, 
    ## the format of the data, and authentication information. It helps to avoid being blocked by the website
    REQ_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
}
    d = {'url': row['url'], 'ip_proxy': row['ip_proxy'], 'last_updated_time_o': row['last_updated_time_o'], 'price_o' : row['price_o'], 'beds_o': row['beds_o'], 'floor_size_o': row['floor_size_o'], 'scraped': row['scraped']}
    with requests.Session() as s:
        adapter = HTTPAdapter(pool_connections=40, pool_maxsize=100)
        s.mount('http://', adapter)
        s.mount('https://',adapter)
        r = s.get(d['url'], proxies={'http':'http://'+d['ip_proxy'], 'https':'http://'+d['ip_proxy']}, timeout = 10, headers= REQ_HEADERS)
        #time.sleep(random.randint(1,2)/10)
        soup = BeautifulSoup(r.content, 'html.parser')
        try:
            d['price'] = float(re.sub("[^\d\.]", "", soup.find('span', attrs={'class', 'price'}).text))
        except:
            pass
        try:
            d['title'] = soup.find('span', attrs={'class', 'postingtitletext'}).find_all('span')[-1].text
        except:
            pass
        try:
            d['floor_plan'] = soup.find('span', attrs={"class", "shared-line-bubble"}).text
        except:
            pass
        # try:
        #     if not pd.isna(soup.find('body')):
        #         d['descr'] = 'Content Removed or Delisted'
        # except:
        #     pass
        try:
            d['descr']  = soup.find('section', attrs={'id' : 'postingbody'}).text.replace('QR Code Link to This Post', '').strip()
        except:
            pass
        try:
            d['p_id'] = int(re.sub("[^0-9]", "", soup.find('div', attrs={'class', 'postinginfos'}).find('p', attrs={'class', 'postinginfo'}).text))
        except:
            pass
        try:
            d['posted_time'] = soup.find_all('time', attrs={'class', 'date timeago'})[0]['datetime']
        except:
            pass
        try:
            d['last_updated_time'] = soup.find_all('time', attrs={'class', 'date timeago'})[-1]['datetime']
        except:
             pass
        try:
            d['map'] = soup.find('div', attrs={'class', 'mapaddress'}).text
        except:
            pass
        try:
            d['lat'] = soup.find('div', attrs={'class', 'mapbox'}).find('div')['data-latitude']
        except:
            pass
        try:
            d['long'] = soup.find('div', attrs={'class', 'mapbox'}).find('div')['data-longitude']
        except:
            pass
        try:
            d['tag1'] = soup.find_all('div', attrs={'class', 'attrgroup'})[0].text.strip()
        except:
            pass
        try:
            d['tag2'] = soup.find_all('div', attrs={'class', 'attrgroup'})[1].text.strip()
        except:
            pass
        try:
            d['tag3'] = soup.find_all('div', attrs={'class', 'attrgroup'})[2].text.strip()
        except:
            pass
        try:
            d['tag4'] = soup.find_all('div', attrs={'class', 'attrgroup'})[3].text.strip()
        except:
            pass
    return d


### scrape listing outside helper functions
def scrape_outside_new(driver):
    """this function takes a BeautifulSoup object obtained from the craigslist gallery page (new format), 
        and try to scrape the information on each gallery card
    Args:
        soup: A BeautifulSoup object, parsed from the html of the gallery page (example: https://newyork.craigslist.org/search/mnh/apa#search=1~gallery~0~0)
    Returns:
        df: a dataframe that contains all the scraped information
    """
    try:
        cards = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@class='gallery-card']")))
    except:
        cards = []
    df = pd.DataFrame()
    for card in cards:
        d = {}
        d['url'] = card.find_element(By.XPATH, ".//a[@tabindex='0']").get_attribute('href')
#         try:
#             d['last_updated_time_o'] = card.find_element(By.XPATH, ".//div[@class='meta']").text.split('·')[0]
#         except:
        d['last_updated_time_o'] = None
        try:
            d['price_o'] = float(re.sub("[^0-9]", "", card.find_element(By.XPATH, ".//span[@class='priceinfo']").text))
        except:
            d['price_o'] = None
        try:
            d['beds_o'] = card.find_element(By.XPATH, ".//span[@class='post-bedrooms']").text
        except:
            d['beds_o'] = None
        try:
            d['floor_size_o'] = card.find_element(By.XPATH, ".//span[@class='post-sqft']").text
        except:
            d['floor_size_o'] = None
        df = pd.concat([df, pd.DataFrame([d], columns=d.keys())], ignore_index = True)
    return df

def scrape_outside_old(driver):
    """this function takes a BeautifulSoup object obtained from the craigslist gallery page (old format), 
        and try to scrape the information on each gallery card
    Args:
        soup: A BeautifulSoup object, parsed from the html of the gallery page (example: https://baltimore.craigslist.org/search/apa)
    Returns:
        df: a dataframe that contains all the scraped information
    """
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    cards = soup.find_all('li', attrs={'class', 'result-row'})
    df = pd.DataFrame()
    for card in cards:
        d = {}
        d['url'] = card.find('a', href=True)['href']
        d['last_updated_time_o'] = datetime.strptime(card.find('time', attrs={'class': 'result-date'})['datetime'],'%Y-%m-%d %H:%M')
        try:
            d['price_o'] = float(re.sub("[^0-9]", "", card.find('span', attrs={'class':'result-price'}).text))
        except:
            d['price_o'] = None
        try:
            housing = card.find('span', attrs={'class':'housing'}).text
            d['beds_o'] = housing.split('-')[0].strip()
            d['floor_size_o'] = housing.split('-')[1].strip()
        except:
            housing = None
            d['beds_o'] = None
            d['floor_size_o'] = None
        df = pd.concat([df, pd.DataFrame([d], columns=d.keys())])
    return df

def scrape_a_page(driver):
    """this function takes a page (page_source of the current page, automated by Selenium), 
        and try to scrape the information for both the new format and old format
    Args:
        page_source: the unparsed page_source of the current page, automated by Selenium
    Returns:
        df: a dataframe that contains all the scraped information
    """
    df = pd.DataFrame()
    # urls_old: urls extracted from craigslist site with old format
    df_old = scrape_outside_old(driver)
    df_new = scrape_outside_new(driver)
    
    if len(df_old) > len(df_new):
        df = df_old
    elif len(df_old) < len(df_new):
        df = df_new
    else:
        df = None
    return df

def distribute_proxies(df, prev_proxies_list, check_num = 200):
    """this function grab the free proxies list from the web, test them and distribute usable ones randomly to all listing waiting to be scraped;
        note that we will compare the working proxies from previous task and skip the testing for these to save time
    Args:
        df: the dataframe with url ready to be scraped, will assign the proxies to each url randomly
        prev_proxies_list: list of proxies that worked in the previous task
        check_num: the maximum number of new proxies to check (use this threshold to save time)
    Returns:
        df: a dataframe that with assigned random working ip proxies for each url
    """
    #df_craiglist = pd.read_csv('craiglist_listing.csv')
    REQ_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }
    test_url = random.choice(list(df['url']))
    print(test_url)
    ## STEP 2: Retrieve a list of free proxies online
    time.sleep(random.randint(1,2)/5)
    with requests.Session() as s:
        retry = Retry(connect=6, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry, pool_connections=40, pool_maxsize=100)
        s.mount('http://', adapter)
        s.mount('https://', adapter)
        resp = s.get('https://free-proxy-list.net/')
    df_proxies = pd.read_html(resp.text)[0]
    df_proxies = df_proxies.loc[(df_proxies['Https'] == 'yes')]
    df_proxies['IP'] = df_proxies['IP Address'].astype(str) + ':' + df_proxies['Port'].astype(str)
    df_proxies['IP Good'] = None
    for index, row in df_proxies.head(check_num).iterrows():
        print("Checking IP #" + str(index + 1))
        ip_add = row.loc['IP']
        if ip_add in prev_proxies_list:
            print("IP {} used successfully in last task".format(ip_add))
            df_proxies.at[index, 'IP Good'] = True
        else:
            with requests.Session() as test_session:
                try:
                    with requests.Session() as test_session:
                        r_test = test_session.get(test_url, proxies={'https': 'http://' + ip_add, 'http': 'http://' + ip_add}, headers=REQ_HEADERS, timeout=10, verify=False)
                        print(r_test.status_code)
                        if r_test.status_code == 200:
                            df_proxies.at[index, 'IP Good'] = True
                except requests.exceptions.ProxyError as e:
                    print("Proxy error:", e)
                except requests.exceptions.SSLError as e:
                    print("SSL error:", e)
                except requests.exceptions.RequestException as e:
                    print("Other error:", e)
    df_proxies_list = list(df_proxies[df_proxies['IP Good'] == True]['IP'])
    print("Length of the proxy list is {}".format(len(df_proxies_list)))
    if len(df_proxies_list) == 0:
        df_proxies_list = prev_proxies_list
    
    df_scraped = df[df['scraped'] == True]
    df_NOTscraped = df[df['scraped'] == False]
    df_NOTscraped['ip_proxy'] = np.random.choice(df_proxies_list, size = len(df_NOTscraped))
    df = pd.concat([df_scraped, df_NOTscraped], ignore_index=True, axis = 0)
    return (df, df_proxies_list)

def retrieve_dataframe(fs, df_craiglist):
    """this function takes the results (called futures) from concurrent futures(multiprocessing), and reformat it to dataframe
    Args:
        fs: list of future objects that contain scraped information done with multi-processing
        df_craiglist: the dataframe to store the scraped information
    Returns:
        df: a dataframe that with the scraped information stored
    """    
    df_scraped = pd.DataFrame()
    for f in fs:
        try:
            df_temp = pd.DataFrame([f.result()], columns = ['price', 'price_o', 'title', 'floor_plan','floor_size_o', 'beds_o', 'url', 'ip_proxy', 'descr', 'p_id', 'posted_time','last_updated_time', 'last_updated_time_o', 'map', 'lat', 'long', 'tag1', 'tag2', 'tag3', 'scraped'])
            df_scraped = pd.concat([df_temp, df_scraped], ignore_index=True, axis = 0)
        except:
            pass     
    try:
        df_UNscraped = df_craiglist[~df_craiglist['url'].isin(df_scraped['url'])]
    #df_scraped['scraped'] = True
        df = pd.concat([df_scraped, df_UNscraped], ignore_index=True, axis = 0)
    except:
        df = df_craiglist
    #df['scraped'] = df['scraped'].fillna(False)
    if 'descr' not in df.columns:
        df['scraped'] = False 
    else:
        df['scraped'] = ~df['descr'].isna()
    return df

def scrape_a_region(region, sub_region, prev_proxies_list, task_num, today, data_dir = "/shared/share_rent/craigslist"):
    """this takes the following arguments, and scrape all the information needed from the craigslist and store them locally
    Args:
        region: string name of the region
        sub_region: string name of the sub_region
        prev_proxies_list: list of working proxies from previous scrape task (scrape_region)
        task_num: # of current task
        today: start date of this week's scrape task
    Returns:
        prev_proxies_list: list of working proxies for the current scrape task(will be used for the next scrape task)
    """    
    scrape_start_time = datetime.now()
    df_craiglist_full = pd.DataFrame()
    ## add new headless options for Selenium (suggested by Benny from Research Support)
    options = Options()
    options.add_argument("-headless") 
    driver = webdriver.Firefox(options = options)
    # #for l in range(0,3000,120):
    if sub_region != None:
        url = 'https://' + region + '.craigslist.org/search/' + sub_region + '/apa'
    else:
        url = 'https://' + region + '.craigslist.org/search/apa'
    driver.get(url)
    time.sleep(3)
    driver.refresh()
    time.sleep(20)
    # scrape of the page 0 before clicking on the "next" button
    df_craiglist_full = scrape_a_page(driver)
    try:
        page_num_info = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="search-toolbars-1"]/div[2]/span'))).text
        page_num_info = re.sub('[^0-9\\s]', '', page_num_info)
        page_num_info = re.sub(' +', ' ', page_num_info)
        page_num_info = page_num_info.split(" ")
        total_p_num = int(np.ceil(int(page_num_info[-1]) // int(page_num_info[-2])))
        for i in range(total_p_num):
            next_button = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="search-toolbars-1"]/div[2]/button[3]')))
            next_button.click()
            time.sleep(6)
            df = scrape_a_page(driver)
            df_craiglist_full = pd.concat([df_craiglist_full, df], ignore_index=True, axis = 0)
    except:
        each_p_num = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '/html/body/section/form/div[3]/div[3]/span[2]/span[3]/span[1]/span[2]'))).text
        total_num = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '/html/body/section/form/div[3]/div[3]/span[2]/span[3]/span[2]'))).text
        total_p_num = int(np.ceil(int(total_num) // int(each_p_num)))
        for i in range(total_p_num):
            try:
                next_button = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="searchform"]/div[3]/div[3]/span[2]/a[3]')))
                next_button.click()
                time.sleep(6)
                df = scrape_a_page(driver)
                df_craiglist_full = pd.concat([df_craiglist_full, df], ignore_index=True, axis = 0)
            except:
                pass
    driver.close()
    df_craiglist_full = df_craiglist_full.drop_duplicates('url')
    df_craiglist_full['ip_proxy'] = None
    df_craiglist_full['scraped'] = False
    # count of finished scrapes 
    count = 0
    # count of remaining scrapes
    remaining_count = 0
    i = 0
    ## previous count of remaining scrapes
    previous_remaining_count = 0
    ## count of iteration with repeating scrapes
    repeating_count_of_remaining = 0
    # get current time
    
    while count < len(df_craiglist_full):  
        print("Performing trial number {} for the dataset".format(i + 1))
        df_craiglist_full, df_proxies_list = distribute_proxies(df_craiglist_full, prev_proxies_list)
        prev_proxies_list = df_proxies_list
        print("Starting multiprocessing")
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(scrape_each_listing, row) for index, row in df_craiglist_full[df_craiglist_full['scraped'] == False].iterrows()]
        df_craiglist_full = retrieve_dataframe(futures, df_craiglist_full)
        print("{} listings are scraped successfully".format(len(df_craiglist_full[df_craiglist_full['scraped'] == True])))
        count = len(df_craiglist_full[df_craiglist_full['scraped'] == True])
        remaining_count = len(df_craiglist_full[df_craiglist_full['scraped'] == False])
        if remaining_count == previous_remaining_count:
            repeating_count_of_remaining += 1
        else:
            repeating_count_of_remaining = 0
        print("and {} listings will be scraped for next trial".format(remaining_count))
        i += 1
        previous_remaining_count = remaining_count

        exit_condition = (repeating_count_of_remaining >= 10) & (remaining_count <= 0.01 * len(df_craiglist_full))
        exit_condition2 = (repeating_count_of_remaining >= 30)
        if exit_condition or exit_condition2:
            # break out the infinite
            print("exit because remaining {} listings cannot be scraped: either delisted or bad proxy".format(remaining_count))
            break
    ## appending to each corresponding dataset
    #today = datetime.today()
    csv_dir = "{}/local_data/{}".format(data_dir, region)
    if not os.path.isdir(csv_dir):
        os.makedirs(csv_dir)
    if sub_region != None:
        csv_filename = csv_dir + "/{}_{}".format(region, sub_region)
    else:
        csv_filename = csv_dir + "/{}".format(region)
    ## adding additional information on the scraped date of the week and region sub region before exporting
    df_craiglist_full['scraped_week'] = today
    df_craiglist_full['region'] = region
    df_craiglist_full['sub_region'] = sub_region
    df_craiglist_full.to_csv(csv_filename + '.csv', header=True, index=False)
    insert_scraped_time_csv(scrape_start_time, region, sub_region, task_num)
    # import general/summary table
    return prev_proxies_list

def scrape(sub_region_df, task_num, today):
    """this is the "main" function that scrapes every region from the top 100 MSA by population
    Args:
        sub_region_df: a dataframe that records what region - sub_region needs to be scraped
        task_num: # of current task
        today: start date of this week's scrape task
    Returns:
        save scraped data locally as csv; ready to be processed and uploaded
    """   
    i = 0
    d_len = len(sub_region_df)
    prev_proxies_list = []
    for index, row in sub_region_df.iterrows():
        print("Start Scraping for {} : {}".format(row['region'], row['sub_region']))
        prev_proxies_list = scrape_a_region(row['region'], row['sub_region'], prev_proxies_list, task_num, today)
        i += 1
        print("{}/{} regions completed".format(i, d_len))