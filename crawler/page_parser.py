from bs4 import BeautifulSoup
import requests
import time
requests.packages.urllib3.disable_warnings()
import page_fetcher
import re
from url_normalize import url_normalize

def fetch_file_content(file_url):
    """ If successful returns a file of type bytes."""
    r = requests.get(file_url)

    if r.status_code == 200:
        return r.content
    else:
        return None

def parse_page_html(domain_url, page_url, page_html):

    soup = BeautifulSoup(page_html, "lxml")

    image_tags = soup.find_all("img")
    # ignore image alternatives, only pick ones with 'src' tag
    image_urls = [ image['src'] for image in image_tags if image.get('src') is not None ]
    
    # find all links
    ## TODO: HREF - JS onclick
    ## TODO: IGNORE OUTGOING LINKS (LEAVING THE BASE URL) - DIFFERENCE BETWEEN BASE AND DOMAIN URL ???
    # link_hrefs = soup.find_all('a', href=True)
    link_urls = []
    file_urls = []
    link_hrefs = soup.find_all('a', href=True)
    for link in link_hrefs:
        href = link.get('href')

        # build dynamic links
        if href[:4] != "http":

            if href[0] == '/':
                href = domain_url + href[1:]
            else:
                href = domain_url + href
        
        if href.endswith('.pdf') or  href.endswith('.doc') or href.endswith('.docx') or href.endswith('.ppt') or href.endswith('.pptx'):
            file_urls.append(href)
        else:
            link_urls.append(href)


    print("IMAGES:")
    for img in image_urls:
        print(img)

    print("FILES:")
    for f in file_urls:
        print(f)

    print("LINKS:")
    for link in link_urls:
        print(link)


    return link_hrefs, image_urls, file_urls



def main():
    # image_site = "https://unsplash.com/search/photos/wallpaper"
    # image_site = "https://www.w3schools.com/jsref/tryit.asp?filename=tryjsref_onclick"
    image_site = "http://www.e-prostor.gov.si/"
    resp_status, image_html = page_fetcher.fetch_page(image_site)
    href_links, images, documents = parse_page_html("https://www.e-prostor.gov.si/", image_site, image_html)
    print("complete")
    
if __name__ == "__main__":
    main()