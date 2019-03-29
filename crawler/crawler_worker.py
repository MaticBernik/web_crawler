#from urllib.parse import urlparse
from urltools import normalize


class Crawler_worker:

    def is_running(self):
        return self.running

    def get_next_URL(self):
        #get next URL from frontier if not empty else None
        #mark URL in frontier as being processed
        #start timer/add timestamp
        cursor=self.cursor
        conn=self.db_conn
        select_statement="""SELECT crawldb.page.id FROM crawldb.frontier INNER JOIN crawldb.page on crawldb.page.id=crawldb.frontier.id  WHERE processing_start_time IS NULL ORDER BY crawldb.frontier.placement FOR UPDATE SKIP LOCKED LIMIT 1"""
        update_statement="""UPDATE crawldb.frontier SET processing_start_time='now' 
                            WHERE id= ("""+select_statement+""")
                            RETURNING crawldb.frontier.id;"""
        cursor.execute(update_statement)
        conn.commit()
        if cursor.rowcount==0:
            return None
        next_page_id=cursor.fetchone()[0]
        select_statement="""SELECT crawldb.page.id,crawldb.page.url FROM crawldb.page WHERE id="""+str(next_page_id)+';'
        cursor.execute(select_statement)
        next_page=cursor.fetchone()
        print('NEXT PAGE: ',next_page)
        return  next_page[1]


    def remove_URL(self,url):
        #remove url from frontier
        #Actually remove or just mark as such??
        return True # REMOVE!!!!
        cursor = self.cursor
        conn = self.db_conn
        normalized_url = normalize(url)
        select_statement = """DELETE FROM crawldb.frontier WHERE id = (SELECT id FROM crawldb.page WHERE url='""" + normalized_url + """');"""
        cursor.execute(select_statement)
        conn.commit()
        return True

    def url_already_processed(self,url,normalize_url=True):
        #check if URL already in column url of table page
        cursor=self.cursor
        if normalize_url:
            normalized_url = normalize(url)
        else:
            normalized_url=url
        select_statement = """SELECT exists (SELECT 1 FROM crawldb.page WHERE url = '"""+normalized_url+"""' LIMIT 1);"""
        cursor.execute(select_statement)
        already_exists=cursor.fetchone()[0]
        return already_exists

    def url_in_frontier(self,url,normalize_url=True):
        # check if URL already in frontier
        cursor = self.cursor
        if normalize_url:
            normalized_url = normalize(url)
        else:
            normalized_url = url
        select_statement = """SELECT exists (SELECT 1 FROM crawldb.frontier WHERE id = (SELECT id from crawldb.page WHERE url = '""" + normalized_url + """') LIMIT 1);"""
        cursor.execute(select_statement)
        already_exists = cursor.fetchone()[0]
        return already_exists

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
        pass
        urls = list({normalize(u) for u in urls})
        urls = [u for u in urls if not self.url_in_frontier(u) and not self.url_already_processed(u)]
        insert_images_statement=""""""
        cursor.execute(insert_images_statement)
        insert_documents_statement=""""""
        cursor.execute(insert_documents_statement)
        insert_urls_statement=""""""
        cursor.execute(insert_urls_statement)
        self.remove_URL(current_url)
        conn.commit()

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
            if self.url_already_processed(current_url):
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



