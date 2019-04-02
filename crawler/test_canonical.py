from urltools import normalize
from urllib.parse import urlparse

from url_normalize import url_normalize

scrappy_links = [
	'http://cs.indiana.edu:80/',
	'http://cs.indiana.edu',
	'http://cs.indiana.edu/People',
	'http://cs.indiana.edu/faq.html#3',
	'http://cs.indiana.edu/a/./../b',
	'http://cs.indiana.edu/index.html',
	'http://cs.indiana.edu/%7Efil',
	'http://cs.indiana.edu/My File.htm',
	'http://CS.INDIANA.EDU/People'
]

for link in scrappy_links:
	print(normalize(link))

print("=================")
# for link in scrappy_links:
	# print(urlparse(link))

print("=================")
# so far best choice
for link in scrappy_links:
	print(url_normalize(link))