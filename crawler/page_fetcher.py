import requests
import time
from selenium import webdriver  
from selenium.webdriver.common.keys import Keys  
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
import shutil

requests.packages.urllib3.disable_warnings()

def initialize_driver():
    options = Options()  
    options.add_argument("--headless")
    options.add_argument("--mute-audio")
    # UBUNTU : whereis chromedriver > /usr/bin/chromedriver
    chrome_driver_location = shutil.which("chromedriver")
    driver = webdriver.Chrome(chrome_driver_location, options=options)
    driver.set_page_load_timeout(10) # fast network
    #driver.set_page_load_timeout(15) # slow network

    return driver

def validate_request_status(url, reconnect_attempts=1, wait_seconds=4):

    while reconnect_attempts > 0:
        try:
            headers = {
                    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
            }
            response = requests.get(url, headers=headers, verify=False, allow_redirects=True, timeout=5)

            if response.status_code == 200:
                if 'sicas-x509si' in response.url:
                    # print("LOGIN SSL REDIRECT")
                    return False, 300 

                return True, response.status_code
        except:
            return False, 400
        finally:
            reconnect_attempts -= 1
            if reconnect_attempts > 0:
                time.sleep(wait_seconds)

    return False, response.status_code

def write_page_html(name, html):

    with open(name+'.html', 'w') as f:
        f.write(html)

def fetch_page(url, worker_id):
    """ GETTING REPLACED BY :  fetch_page_with_driver """
    valid_url, response_code = validate_request_status(url)
    page_html = None

    if valid_url:
        try:
            chrome_driver = initialize_driver()
            # time.sleep(2)
            chrome_driver.get(url)
            wait = WebDriverWait(chrome_driver, 5)
            page_html = chrome_driver.page_source
        except:
            print(worker_id, " failed webdriver for page: " , url)
        finally:
            chrome_driver.quit()
    else:
        print(worker_id, "  INVALID URL - skipping - reponse code: ", response_code, " @ ", url)
    
    return response_code, page_html

def fetch_page_with_driver(url, worker_id, driver):

    valid_url, response_code = validate_request_status(url)
    page_html = None

    if valid_url:
        try:
            # time.sleep(2)
            driver.get(url)
            page_html = driver.page_source
        except:
            # print(worker_id, " failed webdriver with code: ", response_code, "  for page: " , url)
            pass
    else:
        # print(worker_id, "  INVALID URL - skipping - reponse code: ", response_code, " @ ", url)
        pass
    
    if response_code > 499 and response_code < 600:
        print("POSSIBLE FAILURE WITH ", worker_id, " REQUEST RESPONSE 5xx : ", response_code, " @url: ", url)

    return response_code, page_html


def is_text_html(url, reconnect_attempts=1, wait_seconds=4):

    while reconnect_attempts > 0:
        try:
            headers = {
                    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
            }
            response = requests.get(url, headers=headers, verify=False, allow_redirects=True, timeout=5)

            if response.status_code == 200:
                content_type = response.headers['content-type']
                if 'text/html' in content_type or 'text/htm' in content_type:
                    return True
        except:
            return False
        finally:
            reconnect_attempts -= 1
            if reconnect_attempts > 0:
                time.sleep(wait_seconds)

    return False

def main():
    page_url = "https://e-uprava.gov.si/"
    # page_url = "http://www.projekt.e-prostor.gov.si/fileadmin/user_upload/Video_vsebine/eProstor_cilj_1_objava.mp4?fbclid=IwAR28WauTwoha--Rqh0cgmMhEswtfJPJwy9IPtktgaYb2it9k96VYbgqAXsg"
    # page_url = "https://evem.gov.si/evem/uporabnik/preusmeriNaPostopek.evem?postopek=prijavaZavarovanjaSp"
    supposed_invadil = "http://evem.gov.si/info/poslujem/zaposlovanje/poslujem/zaposlovanje/odsotnost-z-dela/"
    url, page = fetch_page(supposed_invadil, 1)
    print(is_text_html(page_url))

if __name__ == "__main__":
    main()