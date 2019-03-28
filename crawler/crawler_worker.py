
class Crawler_worker:

    def is_running(self):
        return self.running

    def get_next_URL(self):
        #get next URL from frontier if not empty else None
        #mark URL in frontier as being processed
        #start timer/add timestamp


        #FIX!!! SELECT AND UPDATE IN SINGLE TRANSACTION? BLOCKING?
        cursor=self.cursor
        conn=self.db_conn
        select_statement='SELECT crawldb.frontier.placement,crawldb.page.id,crawldb.page.url FROM crawldb.frontier INNER JOIN crawldb.page on crawldb.page.id=crawldb.frontier.id  WHERE processing_start_time IS NULL ORDER BY crawldb.frontier.placement limit 1;'
        cursor.execute(select_statement)
        next_page=cursor.fetchone()
        if next_page is None:
            return next_page
        update_statement="""UPDATE crawldb.frontier SET processing_start_time='now' WHERE id="""+str(next_page[1])+';'
        cursor.execute(update_statement)
        conn.commit()
        print('NEXT PAGE: ',next_page)
        return  next_page[2]


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

    def run(self):
        print('WORKER RUNNING..')
        self.running = True
        while True:
            current_url = self.get_next_URL()
            images = []
            documents = []
            urls = []
            # canonicalize URL
            if current_url is None:
                break
            if url_already_processed(current_url):
                self.remove_URL(current_url)
                continue
            self.process_robots_file(current_url)
            content = self.get_page()
            content_type = get_content_type()
            if content_type == 'image':
                images.append(content)
            elif content_type == 'document':
                documents.append(content)
            elif content_type == 'html':
                page_hash = get_hash(content)
                if self.duplicate_page(page_hash):
                    self.remove_URL()
                    continue
                img, href, docs = self.parse_page(content)
                images += img
                documents += docs
                urls += href
            self.write_to_DB(current_url=current_url, images=images, documents=documents, urls=urls)
        self.cursor.close()
        self.running = False

    def __init__(self, db_conn):
        self.db_conn=db_conn
        self.cursor=db_conn.cursor()



