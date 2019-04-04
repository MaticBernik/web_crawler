import requests
import time
from selenium import webdriver  
from selenium.webdriver.common.keys import Keys  
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
import shutil
from selenium.webdriver.support.ui import WebDriverWait

requests.packages.urllib3.disable_warnings()

def initialize_driver():
    options = Options()  
    options.add_argument("--headless")
    options.add_argument("--mute-audio")
    # UBUNTU : whereis chromedriver > /usr/bin/chromedriver
    chrome_driver_location = shutil.which("chromedriver")
    driver = webdriver.Chrome(chrome_driver_location, options=options)
    driver.set_page_load_timeout(5)

    return driver

def validate_request_status(url, reconnect_attempts=1, wait_seconds=4):

    while reconnect_attempts > 0:
        
        headers = {
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
        }
        response = requests.get(url, headers=headers, verify=False, allow_redirects=True, timeout=5)
        
        if response.status_code == 200:
            return True, response.status_code
        else:
            if reconnect_attempts > 1:
                time.sleep(wait_seconds)
            reconnect_attempts -= 1

    return False, response.status_code

def write_page_html(name, html):

    with open(name+'.html', 'w') as f:
        f.write(html)

def fetch_page(url, number_of_attemtps=1):

    valid_url, response_code = validate_request_status(url)
    page_html = None

    if valid_url:
        while number_of_attemtps > 0:
            try:
                chrome_driver = initialize_driver()
                # time.sleep(2)
                chrome_driver.get(url)
                wait = WebDriverWait(chrome_driver, 5)
                page_html = chrome_driver.page_source
                chrome_driver.close()
                break
            except:
                time.sleep(2)
            finally:
                number_of_attemtps -= 1
    else:
        print("INVALID URL : ", url)
    return response_code, page_html


def main():
    page_url = "https://e-uprava.gov.si/"
    # page_url = "http://www.projekt.e-prostor.gov.si/fileadmin/user_upload/Video_vsebine/eProstor_cilj_1_objava.mp4?fbclid=IwAR28WauTwoha--Rqh0cgmMhEswtfJPJwy9IPtktgaYb2it9k96VYbgqAXsg"
    url, page = fetch_page(page_url)



if __name__ == "__main__":
    main()