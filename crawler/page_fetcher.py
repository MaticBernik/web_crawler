import requests
import time
from selenium import webdriver  
from selenium.webdriver.common.keys import Keys  
from selenium.webdriver.chrome.options import Options
import shutil

requests.packages.urllib3.disable_warnings()

def initialize_driver():
    options = Options()  
    options.add_argument("--headless")
    options.add_argument("--mute-audio")
    # UBUNTU : whereis chromedriver > /usr/bin/chromedriver
    chrome_driver_location = shutil.which("chromedriver")
    driver = webdriver.Chrome(chrome_driver_location, options=options)

    return driver

def validate_request_status(url, reconnect_attempts=3, wait_seconds=4):

    while reconnect_attempts > 0:
        
        headers = {
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
        }
        response = requests.get(url, headers=headers, verify=False, allow_redirects=True, timeout=10)
        
        if response.status_code == 200:
            return True, response.status_code
        else:
            time.sleep(wait_seconds)
            reconnect_attempts -= 1

    return False, response.status_code

def write_page_html(name, html):

    with open(name+'.html', 'w') as f:
        f.write(html)

def fetch_page(url, number_of_attemtps=3):

    valid_url, response_code = validate_request_status(url)
    page_html = None

    if valid_url:
        while number_of_attemtps > 0:
            try:
                chrome_driver = initialize_driver()
                time.sleep(2)
                chrome_driver.get(url)
                page_html = chrome_driver.page_source
                driver.close()
                break
            except:
                number_of_attemtps -= 1
                time.sleep(10)

    return response_code, page_html


def main():
    page_url = "https://e-uprava.gov.si/"
    fetch_page(page_url)
    print("Complete")


if __name__ == "__main__":
    main()