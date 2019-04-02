file_urls=['a']
file_urls[0] = "http://www.google.si/"
print(fetch_file_content(file_urls[0]))
print(type(file_urls[0]))