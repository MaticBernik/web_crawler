import requests
import time
from selenium import webdriver  
from selenium.webdriver.common.keys import Keys  
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
import shutil


test_links = """
http://www.e-prostor.gov.si/piskotki/
http://www.e-prostor.gov.si/
http://www.e-prostor.gov.si/
http://www.e-prostor.gov.si/o-portalu/
http://www.e-prostor.gov.si/kontakt/
http://www.e-prostor.gov.si/o-portalu/
http://www.e-prostor.gov.si/kontakt/
http://www.e-prostor.gov.si/zbirke-prostorskih-podatkov/zbirka-vrednotenja-nepremicnin/
http://www.e-prostor.gov.si/dostop-do-podatkov/dostop-do-podatkov/
http://www.e-prostor.gov.si/brezplacni-podatki/
http://www.e-prostor.gov.si/metapodatki/
http://www.e-prostor.gov.si/aplikacije/
http://www.e-prostor.gov.si/informacije/
http://www.e-prostor.gov.si/
https://egp.gu.gov.si/egp/
https://prostor3.sigov.si/pgp/index.jsp
https://gis.gov.si/ezkn/
http://prostor3.gov.si/zem_imena/zemImena.jsp
http://sitranet.si/sitrik.html
http://sitranet.si
"""
test_links = test_links.split("\n")

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

def initialize_driver():
    options = Options()  
    # options.add_argument("--headless")
    options.add_argument("--mute-audio")
    # UBUNTU : whereis chromedriver > /usr/bin/chromedriver
    chrome_driver_location = shutil.which("chromedriver")
    driver = webdriver.Chrome(chrome_driver_location, options=options)
    driver.set_page_load_timeout(10) # fast network
    #driver.set_page_load_timeout(15) # slow network

    return driver

def fetch_page(url, worker_id):

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


def fetch_page_with_driver(url, worker_id, chrome_driver):

    valid_url, response_code = validate_request_status(url)
    page_html = None

    if valid_url:
        try:
            # time.sleep(2)
            chrome_driver.get(url)
            # wait = WebDriverWait(chrome_driver, 5)
            page_html = chrome_driver.page_source
        except:
            print(worker_id, " failed webdriver with code: ", response_code, "  for page: " , url)
    else:
        print(worker_id, "  INVALID URL - skipping - reponse code: ", response_code, " @ ", url)
    
    return response_code, page_html



# fecth_data = []
# start = time.time()
# for i, link in enumerate(test_links):
#     scrapped_sited = fetch_page(link, 1)
#     fecth_data.append(scrapped_sited)
#     print(i)
# end = time.time() - start
# print("Runtime : ", end)      #52 sec
# print(len(fecth_data))

fecth_data_w = []
start = time.time()
driver = initialize_driver()
for i, link in enumerate(test_links):
    scrapped_sited = fetch_page_with_driver(link, 1, driver)
    fecth_data_w.append(scrapped_sited)
    print(i)
end = time.time() - start
print("Runtime with: ", end)   # 13 sec
print(len(fecth_data_w))

