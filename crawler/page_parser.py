from bs4 import BeautifulSoup
import requests
import time
requests.packages.urllib3.disable_warnings()
import page_fetcher
import re
from url_normalize import url_normalize
from urllib.parse import urljoin
from base64 import b64decode


def validate_or_join_url(page_url, link_url):
    
    if link_url[:4] == "http":
        return link_url
    else:
        if link_url[:3] == "www":
            return "http://" + link_url
        else:
            return urljoin(page_url, link_url)

def fetch_base64_file(base64_url):

    try:
        header, encoded = base64_url.split(",", 1)
        data = b64decode(encoded)
        r_code = 200
        # extension = header.split("/")[-1][:3]
    except:
        data = None
        r_code = 404

    return r_code, data

def fetch_file_content(file_url):
    """ If successful returns a file of type bytes."""

    # base64 data encoding
    if file_url[:4] == "data:":
        return fetch_base64_file(file_url)
    
    try:
        headers = {
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
        }
        r = requests.get(file_url, headers=headers, timeout=5)

        if r.status_code == 200:
            return r.status_code, r.content
        else:
            return r.status_code, None
    except:
        return 404, None

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
    resp_status, image_html = page_fetcher.fetch_page(image_site, 1)
    images, documents, link_hrefs = parse_page_html(image_site, image_html)
    print("complete")
    data_uri = "data:image/png;base64,iVBORw0KGg..."
    
    # test file fetch
    img_url = "https://pbs.twimg.com/profile_images/875766338081312772/m1UiRwLF_400x400.jpg"
    # print(fetch_file_content(img_url))


    # base64 test
    img_url_base64 = "data:image/png;base64, iVBORw0KGgoAAAANSUhEUgAAAAUAAAAFCAYAAACNbyblAAAAHElEQVQI12P4//8/w38GIAXDIBKE0DHxgljNBAAO9TXL0Y4OHwAAAABJRU5ErkJggg=="
    rcode, data = fetch_base64_file(img_url_base64)
    print(rcode)

if __name__ == "__main__":
    main()