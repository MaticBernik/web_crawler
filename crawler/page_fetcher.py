import requests
import time
from selenium import webdriver  
from selenium.webdriver.common.keys import Keys  
from selenium.webdriver.chrome.options import Options  

requests.packages.urllib3.disable_warnings()

def initialize_driver():
    options = Options()  
    options.add_argument("--headless")

    # UBUNTU : whereis chromedriver > /usr/bin/chromedriver
    chrome_driver_location = '/usr/bin/chromedriver'
    driver = webdriver.Chrome(chrome_driver_location, options=options)

    return driver

def validate_request_status(url, reconnect_attempts=3, wait_seconds=4):

    while reconnect_attempts > 0:

        response = requests.get(url, verify=False, allow_redirects=True, timeout=10)
        
        if response.status_code == 200:
            return True, response.status_code
        else:
            time.sleep(wait_seconds)
            reconnect_attempts -= 1

    return False, response.status_code

def write_page_html(name, html):

    with open(name+'.html', 'w') as f:
        f.write(html)

def fetch_page(url):

    valid_url, response_code = validate_request_status(url)
    
    if valid_url:
        chrome_driver = initialize_driver()
        chrome_driver.get(url)
        page_html = chrome_driver.page_source
        # write_page_html("govtest", page_html)
    else:
        return False, None

    return response_code, page_html


def main():
    page_url = "https://e-uprava.gov.si/"
    fetch_page(page_url)


if __name__ == "__main__":
    main()