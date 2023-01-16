Craigslist Rent Project Instruction
==============================

This is the scraper codespace for the craigslist rent project: the Python scripts in this codespace are used to script all needed information on each listing from Craigslist for the Top 100 MSA.

To run the script, run the following command on terminal: python scrape.py taskNum -d scraped_date;
If on the grid, then we can add extra options to expediate the process by using more threads and memory: anapy3 scrape.py taskNum -d scraped_date --grid_mem=50G --grid_ncpus=32.


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
        │   └── scrape.py          <- Script to start the scrape
        │
        ├── analysis               <- Scripts to perform analysis needed
            └── time_on_market.ipynb
    
    


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
