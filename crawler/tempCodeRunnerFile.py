xmlDict = {}

r = requests.get("http://www.site.co.uk/sitemap.xml")
xml = r.text

soup = BeautifulSoup(xml)
sitemapTags = soup.find_all("sitemap")

print "The number of sitemaps are {0}".format(len(sitemapTags))

for sitemap in sitemapTags:
    xmlDict[sitemap.findNext("loc").text] = sitemap.findNext("lastmod").text

print xmlDict

