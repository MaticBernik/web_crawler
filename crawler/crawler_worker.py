
class crawler_worker:

    def is_running(self):
        return self.running

    def get_next_URL(self):
        #get next URL from frontier if not empty else None
        #mark URL in frontier as being processed
        #start timer/add timestamp
        pass

    def remove_URL(self,url):
        #remove url from frontier
        return True

    def url_already_processed(self,url):
        #check if URL already in column url of table page
        pass

    def process_robots_file(self,url):
        #extract domain base url
        #check for existance of robots.txt
        #process robots.txt (User-agent, Allow, Disallow, Crawl-delay and Sitemap)??
        #If a sitemap is defined shuld all the URLs defined within it be added to the frontier exclusively or additionaly
        pass

    def get_page(self):
        #download and render page ??Selenium??
        pass

    def get_hash(self):
        #hash content LSH
        pass

    @staticmethod
    def get_content_type(content):
        #check if img/document/html...
        pass

    def write_to_DB(self,current_url,images,documents,urls):
        #WITHIN SINGLE TRANSACTION!!!
        #write new data to database
        #and remove current_url from frontier
        self.remove_URL(current_url)

    def parse_page(self,content):
        #parse html page and return three lists:
        #list of images, list of urls and list of documents
        pass

    def __init__(self, db_conn):
        self.db_conn=db_conn
        self.running=True

        while True:
            current_url = self.get_next_URL()
            images=[]
            documents=[]
            urls=[]
            #canonicalize URL
            if current_url is None:
                break
            if url_already_processed(current_url):
                self.remove_URL(current_url)
                continue
            self.process_robots_file(current_url)
            content=self.get_page()
            content_type=get_content_type()
            if content_type=='image':
                images.append(content)
            elif content_type=='document':
                documents.append(content)
            elif content_type=='html':
                page_hash=get_hash(content)
                if self.duplicate_page(page_hash):
                    self.remove_URL()
                    continue
                img,href,docs=self.parse_page(content)
                images+=img
                documents+=docs
                urls+=href
            self.write_to_DB(current_url=current_url,images=images,documents=documents,urls=urls)
        self.running=False


