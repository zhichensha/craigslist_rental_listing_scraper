
# Craigslist Scraping Suite

## Overview
This suite of Python scripts (`main.py`, `scrape_batch.py`, `utils.py`) automates the process of scraping Craigslist for real estate listings across different regions. The suite manages tasks ranging from data extraction, preprocessing, to storing scraped data.

## Features
- **Data Extraction**: Scrapes real estate listings from Craigslist using Selenium and BeautifulSoup for dynamic and static content handling respectively.
- **Task Management**: Manages scraping tasks based on pre-defined schedules and records completion times.
- **Data Preprocessing**: Cleans and preprocesses scraped data, including geocoding and zipcode assignment using Census data.
- **Efficiency and Scalability**: Implements proxy management and multiprocessing to enhance scraping efficiency and handle large volumes of data.

## Components
1. **main.py**: Manages the scraping process, including setting up drivers, navigating pages, and extracting listing details.
2. **scrape_batch.py**: Coordinates batch scraping operations, including reading schedules, handling proxies, and initiating scraping sessions based on tasks.
3. **utils.py**: Contains utility functions for data handling, such as dataframe creation, preprocessing, geocoding, and command-line argument parsing.

## Usage
- Prepare environment with Python 3.x and required libraries: Selenium, pandas, BeautifulSoup, requests, and geopandas.
- Set up task schedules and database credentials as needed.
- Run `scrape_batch.py` to start scheduled scraping tasks.

## Requirements
- Python 3.x
- Selenium, pandas, BeautifulSoup, requests, geopandas, tqdm
- Firefox WebDriver (or any compatible driver for Selenium)

## Installation
Ensure Python and all dependencies are installed:
```bash
pip install selenium pandas beautifulsoup4 requests geopandas tqdm
```
Download and configure the appropriate WebDriver for your browser.

## Configuration
- Configure scraping tasks in `scrape_batch.py`.
- Set proxy and database details in respective configurations.

## Example
This suite is particularly useful for real estate analysts, data scientists, or businesses that rely on up-to-date property listings from Craigslist for market analysis or other applications.

Project Organization
------------

    ├── README.md          <- The top-level README for developers using this project.
    ├── local_data         <- directory to store weekly scraped data (will be overwritten by the next week's)
    │   ├── akroncanton            <- folder containing scraped data in csv for akroncanton
    │   ......
    │   └── worchester             <- folder containing scraped data in csv for worchester
    │
    ├── reference_files            <- folder containing reference files
    │   └── top100_msa.xlsx        <- Excel file containing records for top 100 msa and corresponding craigslist site (can be replaced)
    │
    ├── credentials                <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── credentials.csv        <- Credentials for accessing the MySQL Database
    │
    └── codes                      <- Source code for use in this project.
        │
        ├── dev                    <- Scripts to scrape data from craigslist
        │   └── utils.py           <- Script containing utility-related functions
        │   ├── main.py            <- Script containing scraping-related functions
        │   └── scrape_batch.py          <- Script to start the scrape (utilizing batch submit on the server)
        │
        ├── analysis               <- Scripts to perform analysis needed
            └── time_on_market.ipynb
    
    


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
