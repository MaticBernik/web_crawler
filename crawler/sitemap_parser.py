from bs4 import BeautifulSoup
import requests
import time
requests.packages.urllib3.disable_warnings()

def request_sitemap_xml(url, reconnect_attempts=3, wait_seconds=4):

    while reconnect_attempts > 0:

        response = requests.get(url, verify=False, allow_redirects=False, timeout=10)
        
        if response.status_code == 200:
            return response.status_code, response.text
        else:
            time.sleep(wait_seconds)
            reconnect_attempts -= 1

    return False, None


def parse_sitemap(url):

    response_code, site_xml = request_sitemap_xml(url)
    
    if response_code:
        soup = BeautifulSoup(site_xml, "lxml")
        url_tags = soup.find_all("url")
        sitemap_urls = [ sitemap.findNext("loc").text for sitemap in url_tags  ]
    else:
        return False, None

    return response_code, sitemap_urls

def parse_sitemap_xml(xml_file):

    if xml_file is not None:
        soup = BeautifulSoup(xml_file, "lxml")
        url_tags = soup.find_all("url")
        sitemap_urls = [ sitemap.findNext("loc").text for sitemap in url_tags  ]
    else:
        return []

    return sitemap_urls


def main():
    sitemap_url = "http://www.e-prostor.gov.si/?eID=dd_googlesitemap"
    r_code, found_sitemaps = parse_sitemap(sitemap_url)
    for site in found_sitemaps:
        print("URL FOUND: ", site)

if __name__ == "__main__":
    main()