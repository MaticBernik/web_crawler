#from urllib.parse import urlparse
from urltools import normalize
from urllib.parse import urlparse
import urllib.robotparser
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

    def url_already_processed(self,url,normalize_url=False):
        #check if URL already in column url of table page
        cursor=self.cursor
        if normalize_url:
            normalized_url = Crawler_worker.normalize_url(url)
        else:
            normalized_url=url
        #select_statement = """SELECT exists (SELECT 1 FROM crawldb.page WHERE url = '"""+normalized_url+"""' LIMIT 1);"""
        select_statement = """SELECT exists (
                                SELECT 1 FROM crawldb.page INNER JOIN crawldb.frontier ON crawldb.page.id=crawldb.frontier.id  
                                WHERE crawldb.frontier.status='done' AND crawldb.page.url = '""" + normalized_url + """' LIMIT 1);"""
        cursor.execute(select_statement)
        already_exists=cursor.fetchone()[0]
        return already_exists

    def url_in_frontier(self,url,normalize_url=False):
        # check if URL already in frontier
        cursor = self.cursor
        if normalize_url:
            normalized_url = Crawler_worker.normalize_url(url)
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

    @staticmethod
    def normalize_url(url):
        return normalize(url)


    def get_current_depth(self,url,normalize_url=False):
        cursor = self.cursor
        if normalize_url:
            normalized_url = Crawler_worker.normalize_url(url)
        else:
            normalized_url = url
        select_statement = """SELECT depth FROM crawldb.frontier WHERE url='"""+normalized_url+"""';"""
        cursor.execute(select_statement)
        current_depth = cursor.fetchone()[0]
        return current_depth

    def process_robots_file(self,url):
        return
        #extract domain base url
        #check for existance of robots.txt
        #process robots.txt (User-agent, Allow, Disallow, Crawl-delay and Sitemap)??
        #If a sitemap is defined shuld all the URLs defined within it be added to the frontier exclusively or additionaly
        cursor=self.cursor
        parsed_uri = urlparse(url)
        domain_url = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        select_statement = """SELECT robots_content,sitemap_content FORM crawldb.site WHERE domain = '"""+domain_url+"""'"""
        cursor.execute(select_statement)
        #RETRIEVE DATA FROM WEBPAGE URL INSTEAD OF DB BUT CACHE LOCALLY BETWEEN THREADS
        if cursor.rowcount > 0:
            robots_content,sitemap_content = cursor.fetchone()
        else:
            robots_url=domain_url+'robots.txt'
            rp = urlib.robotparser.RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            #WRITE DOMAIN DATA TO DATABASE

            #add sitemap urls to (list to be later added to) frontier??
        return rp

    def get_page(self,url,useragent):
        #download and render page ??Selenium??
        pass

    def get_hash(self,content):
        #hash content LSH
        pass


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

    def duplicate_page(self,page_hash):
        #check if page with specified page_hash is already in DB
        pass


    def parse_page(self,content):
        #parse html page and return three lists:
        #list of images, list of hrefs and list of documents
        #each list contains URLs (or tuples??)
        images=[]
        documents=[]
        hrefs=[]
        return images,documents,hrefs

    def early_stop_condition(self):
        select_statement = """SELECT count(*) 
                              FROM crawldb.page
                              WHERE page_type_code='HTML';"""
        self.cursor.execute(select_statement)
        html_page_count=self.cursor.fetchone()
        return html_page_count[0] > 100000

    def run(self):
        print(self.id+' RUNNING..')
        self.running = True
        while True:
            images = []
            documents = []
            hrefs = []
            ##### CHECK IF 100.000 WEB PAGES ALREADY PROCESSED #####
            if self.early_stop_condition():
                print(self.id+'EARLY-STOP CONDITION REACHED ...exiting!')
                break
            ##### TRY TO GET NEXT JOB/URL (exit after 3 retries) #####
            for retry in range(3):
                current_url = self.get_next_URL()
                if current_url is not None:
                    break
                else:
                    print(self.id+' without URL job...retrying in 1s...')
                    time.sleep(1)
            else:
                break
            ##### CHECK IF NEW JOB/URL WAS ALREADY PROCESSED (if it was, mark job as done) #####
            if self.url_already_processed(current_url):
                self.processing_done_URL(current_url)
                continue

            time.sleep(3)  # Simulate processing time...REMOVE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

            ##### PROCESS ROBOTS FILE (returns robotparser object) #####
            rp=self.process_robots_file(current_url)

            ##### RENDER/DOWNLOAD WEBPAGE #####
            useragent="*"
            content = self.get_page(url=current_url,useragent=useragent)

            ##### CHECK IF PAGE CONTENT IMPLIES ALREADY PROCESSED PAGE (if it was, mark job as done) #####
            page_hash = self.get_hash(content)
            if self.duplicate_page(page_hash):
                self.remove_URL(current_url)
                continue

            ##### PARSE WEBPAGE AND EXTRACT IMAGES,DOCUMENTS AND HREFS #####
            images_tmp, documents_tmp, hrefs_tmp = self.parse_page(content)
            images += images_tmp
            documents += documents_tmp
            hrefs += hrefs_tmp

            ##### FILTER URLS BASED ON ROBOTS FILE #####
            images = [image_url for image_url in images if rp.can_fetch(useragent,image_url)]
            documents = [document_url for document_url in documents if rp.can_fetch(useragent, document_url)]
            hrefs = [href_url for href_url in hrefs if rp.can_fetch(useragent, href_url)]

            ##### NORMALIZE URLS #####
            images = [Crawler_worker.normalize_url(image_url) for image_url in images]
            documents = [Crawler_worker.normalize_url(document_url) for document_url in documents]
            hrefs = [Crawler_worker(href_url) for href_url in hrefs]

            ##### WRITE NEW DATA TO DB IN SINGLE TRANSACTION #####
            self.write_to_DB(current_url=current_url, images=images, documents=documents, urls=hrefs)
        print(self.id+' exiting!')
        self.cursor.close()
        self.running = False

    def __init__(self, db_conn, frontier_seed_urls, id='WORKER'):
        self.db_conn=db_conn
        self.cursor=db_conn.cursor()
        self.id=id
        self.frontier_seed_urls=frontier_seed_urls



