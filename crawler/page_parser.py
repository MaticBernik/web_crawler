from bs4 import BeautifulSoup
import requests
import time
requests.packages.urllib3.disable_warnings()
# import page_fetcher
import re
from url_normalize import url_normalize
from urllib.parse import urljoin

def validate_or_join_url(page_url, link_url):
    
    if link_url[:4] == "http":
        return link_url
    else:
        if link_url[:3] == "www":
            return "http://" + link_url
        else:
            return urljoin(page_url, link_url)

def fetch_file_content(file_url):
    """ If successful returns a file of type bytes."""
    r = requests.get(file_url)

    if r.status_code == 200:
        return r.content
    else:
        return None

def print_page_links(image_urls, file_urls, link_urls):
    print("IMAGES:")
    for img in image_urls:
        print(img)

    print("FILES:")
    for f in file_urls:
        print(f)

    print("LINKS:")
    for link in link_urls:
        print(link)
    
def parse_page_html(page_url, page_html):

    
    
    soup = BeautifulSoup(page_html, "lxml")

    try:
        image_tags = soup.find_all("img")
        # ignore image alternatives, only pick ones with 'src' tag
        image_urls = [ validate_or_join_url(page_url, image['src']) for image in image_tags if image.get('src') is not None ]
    except:
        image_urls = None
    
    try:
        # find all links
        ## JS - onclick (should provide href)
        link_urls = []
        file_urls = []
        link_hrefs = soup.find_all('a', href=True)
        for link in link_hrefs:
            href = link.get('href')

            # ignore empty javascript
            if "javascript:void(0)" in href:
                continue

            href = validate_or_join_url(page_url, href)

            if href.endswith('.pdf') or  href.endswith('.doc') or href.endswith('.docx') or href.endswith('.ppt') or href.endswith('.pptx') or href.endswith('.PDF')  or  href.endswith('.DOC') or href.endswith('.DOCX') or href.endswith('.PPT') or href.endswith('.PPTX'):
                file_urls.append(href)
            else:
                link_urls.append(href)
    except:
        link_urls = None
        file_urls = None

    if image_urls == None:
        image_urls = []
    if file_urls == None:
        file_urls = []
    if link_urls == None:
        link_urls = []

    return image_urls, file_urls, link_urls

def main():

    # image_site = "https://unsplash.com/search/photos/wallpaper"
    # image_site = "https://www.w3schools.com/jsref/tryit.asp?filename=tryjsref_onclick"
    image_site = "http://www.e-prostor.gov.si/"
    resp_status, image_html = page_fetcher.fetch_page(image_site)
    href_links, images, documents = parse_page_html(image_site, image_html)
    print("complete")


if __name__ == "__main__":
    main()