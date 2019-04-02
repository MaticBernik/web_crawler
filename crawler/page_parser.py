from bs4 import BeautifulSoup
import requests
import time
requests.packages.urllib3.disable_warnings()
import page_fetcher
import re
from url_normalize import url_normalize

def parse_page_html(base_url, page_html):

    soup = BeautifulSoup(page_html, "lxml")

    image_tags = soup.find_all("img")
    # ignore image alternatives, only pick ones with 'src' tag
    image_urls = [ image['src'] for image in image_tags if image.get('src') is not None ]
    
    # find all links
    ## TODO: IGNORE OUTGOING LINKS (LEAVING THE BASE URL) - DIFFERENCE BETWEEN BASE AND DOMAIN URL ???
    link_hrefs = soup.find_all('a', href=True)
    link_urls = []
    for link in link_hrefs:
        href = link.get('href')
        # build dynamic hrefs
        if href[:4] != "http":
            # href = url_normalize(base_url+href)
            href = base_url + href[1:]
        
        link_urls.append(href)

    ## TODO: HREF - JS onclick
    ## TODO: FILE LINKS (pdf, docx,..)

    
    return link_hrefs, image_urls, None



def main():
    # image_site = "https://unsplash.com/search/photos/wallpaper"
    image_site = "https://www.w3schools.com/jsref/tryit.asp?filename=tryjsref_onclick"
    resp_status, image_html = page_fetcher.fetch_page(image_site)
    href_links, images, documents = parse_page_html("https://unsplash.com/", image_html)
    print("complete")

if __name__ == "__main__":
    main()