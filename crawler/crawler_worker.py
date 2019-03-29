#from urllib.parse import urlparse
from urltools import normalize
import time



class Crawler_worker:

    def is_running(self):
        return self.running

    def get_next_URL(self):
        #get next URL from frontier if not empty else None
        #mark URL in frontier as being processed
        #start timer/add timestamp
        cursor=self.cursor
        conn=self.db_conn
        #ENSURE BREADTH-FIRST STRATEGY
        select_statement="""SELECT MIN(depth) from crawldb.frontier WHERE status='waiting'"""
        select_statement="""SELECT crawldb.page.id 
                            FROM crawldb.frontier INNER JOIN crawldb.page ON crawldb.page.id=crawldb.frontier.id  
                            WHERE status = 'waiting' AND processing_start_time IS NULL AND depth = ("""+select_statement+""")
                            ORDER BY crawldb.frontier.placement FOR UPDATE SKIP LOCKED LIMIT 1"""
        update_statement="""UPDATE crawldb.frontier SET processing_start_time='now', status='processing' 
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
        print(self.id+': NEXT PAGE: ',next_page)
        return  next_page[1]


    def remove_URL(self,url):
        #remove url from frontier
        #Actually remove or just mark as such??
        return True # REMOVE!!!!
        cursor = self.cursor
        conn = self.db_conn
        normalized_url = normalize(url)
        delete_statement = """DELETE FROM crawldb.frontier 
                              WHERE id = (SELECT id FROM crawldb.page WHERE url='""" + normalized_url + """');"""
        cursor.execute(delete_statement)
        conn.commit()
        return True

    def processing_done_URL(self,url):
        cursor = self.cursor
        conn = self.db_conn
        normalized_url = normalize(url)
        update_statement = "UPDATE crawldb.frontier SET status='done' WHERE id = (SELECT id FROM crawldb.page WHERE url = '" + normalized_url + "' LIMIT 1);"
        cursor.execute(update_statement)
        conn.commit()
        return True

    def url_already_processed(self,url,normalize_url=True):
        #check if URL already in column url of table page
        cursor=self.cursor
        if normalize_url:
            normalized_url = normalize(url)
        else:
            normalized_url=url
        #select_statement = """SELECT exists (SELECT 1 FROM crawldb.page WHERE url = '"""+normalized_url+"""' LIMIT 1);"""
        select_statement = """SELECT exists (
                                SELECT 1 FROM crawldb.page INNER JOIN crawldb.frontier ON crawldb.page.id=crawldb.frontier.id  
                                WHERE crawldb.frontier.status='done' AND crawldb.page.url = '""" + normalized_url + """' LIMIT 1);"""
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
        select_statement = """SELECT exists (
                                SELECT 1 FROM crawldb.frontier 
                                WHERE id = (
                                    SELECT id from crawldb.page WHERE url = '""" + normalized_url + """') 
                                LIMIT 1);"""
        cursor.execute(select_statement)
        already_exists = cursor.fetchone()[0]
        return already_exists

    def get_current_depth(self,url,normalize_url=True):
        cursor = self.cursor
        if normalize_url:
            normalized_url = normalize(url)
        else:
            normalized_url = url
        select_statement = """SELECT depth FROM crawldb.frontier WHERE url='"""+normalized_url+"""';"""
        cursor.execute(select_statement)
        current_depth = cursor.fetchone()[0]
        return current_depth

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
        #for URLs: DEPTH = DEPTH +1
        return #REMOVE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        conn=self.db_conn
        cursor=self.cursor
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
        print(self.id+' RUNNING..')
        self.running = True
        while True:

            images = []
            documents = []
            urls = []
            # canonicalize URL
            for retry in range(3):
                current_url = self.get_next_URL()
                if current_url is not None:
                    break
                else:
                    print(self.id+' without URL job...retrying in 1s...')
                    time.sleep(1)
            else:
                break
            if self.url_already_processed(current_url):
                self.processing_done_URL(current_url)
                continue
            time.sleep(3)  # Simulate processing time...REMOVE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            self.process_robots_file(current_url)
            content = self.get_page()
            content_type = Crawler_worker.get_content_type(content)
            if content_type == 'image':
                images.append(content)
            elif content_type == 'document':
                documents.append(content)
            elif content_type == 'html':
                page_hash = get_hash(content)
                if self.duplicate_page(page_hash):
                    self.remove_URL(current_url)
                    continue
                img, href, docs = self.parse_page(content)
                images += img
                documents += docs
                urls += href
            self.write_to_DB(current_url=current_url, images=images, documents=documents, urls=urls)
        print(self.id+' exiting!')
        self.cursor.close()
        self.running = False

    def __init__(self, db_conn, id='WORKER'):
        self.db_conn=db_conn
        self.cursor=db_conn.cursor()
        self.id=id



