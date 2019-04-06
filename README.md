# Standalone hyper-threaded multiworker web crawler
#### Adapted for the use on .gov.si domain

A web crawler, enabled to extract any documents, images and links from a gov.si websites (or any other given websites). It uses selenium chrome based headless browser, which makes it javascript friendly.

## Installation 
### Prerequisites
Python 3.6/3.7 (tested on linux - Ubuntu 18.04)

#### Scraping
```
beautifulsoup4
selenium
requests
lxml

```
#### Processing
```
datasketch
psycopg2
threading
json
```
#### Visualizations
```
seaborn
```


## Running
Run crawler.py

important paramteres:
NUMBER_OF_WORKERS #default 8 should equal number of threads
DOMAIN_DEFAULT_MINIMUM_SECONDS_BETWEEN_REQUESTS $ default 4 respected delay - good ethics

## Analysis

## Data storage (PostgreSQL DB)

SQL script defining crawldb database structure crawldb.sql was downloaded from http://zitnik.si/teaching/wier/data/pa1/crawldb.sql and modified

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
