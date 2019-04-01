import robotparser
#from urllib import robotparser

url='http://e-prostor.gov.si/robots.txt'
rp = robotparser.RobotFileParser()
rp.set_url(url)
rp.read()
print(rp.entries)
print(rp.sitemaps)
#print(rp.can_fetch("*",'http://e-prostor.gov.si/fileadmin/global/'))
#print(rp.default_entry)
#url='e-prostor.gov.si/robots.txt'