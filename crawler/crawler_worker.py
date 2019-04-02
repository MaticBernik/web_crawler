from urltools import normalize
import urllib
from urllib.parse import urlparse
from threading import Lock
import robotparser
import time
import page_fetcher
import sitemap_parser



class Crawler_worker:
    cache_robots={}
    cache_robots_lock=Lock()

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

    @staticmethod
    def is_gov_url(url):
        #FIX-IT
        return ".gov.si" in url


    def get_current_depth(self,url,normalize_url=False):
        cursor = self.cursor
        if normalize_url:
            normalized_url = Crawler_worker.normalize_url(url)
        else:
            normalized_url = url
        select_statement = """SELECT depth 
                              FROM crawldb.frontier INNER JOIN crawldb.page ON crawldb.frontier.id = crawldb.page.id
                              WHERE url='"""+normalized_url+"""';"""
        cursor.execute(select_statement)
        current_depth = cursor.fetchone()[0]
        return current_depth



    def get_robots(self,url):
        #extract domain base url
        #check for existance of robots.txt
        #process robots.txt (User-agent, Allow, Disallow, Crawl-delay and Sitemap)??
        #If a sitemap is defined shuld all the URLs defined within it be added to the frontier exclusively or additionaly
        #If site not already in DB, write it there
        #else just try to find site's RP object in local cache
        cursor=self.cursor
        conn=self.db_conn
        parsed_uri = urlparse(url)
        domain_url = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        ##### restore from cache if stored else create #####
        Crawler_worker.cache_robots_lock.acquire()
        if domain_url in Crawler_worker.cache_robots:
            rp=Crawler_worker.cache_robots[domain_url]
        else:
            robots_url = domain_url + 'robots.txt'
            rp = robotparser.RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            Crawler_worker.cache_robots[domain_url]=rp
        Crawler_worker.cache_robots_lock.release()
        return rp

    @staticmethod
    def read_page(url):
        """Reads the URL and feeds it to the parser."""
        """Copied and adapted from robotparser.py"""
        try:
            f = urllib.request.urlopen(url)
        except urllib.error.HTTPError as err:
            if err.code in (401, 403):
                #Forbidden,unauthorized
                pass
            elif err.code >= 400 and err.code < 500:
                #retry?
                pass
        else:
            raw = f.read()
            raw = raw.decode("utf-8")
            return raw

    def get_page(self,url,useragent):
        response_code, page_html = page_fetcher.fetch_page(url)
        return response_code, page_html

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

    @staticmethod
    def process_sitemap(sitemap):
        '''
        process sitemap document and extract hrefs/urls
        :param sitemap: sitemap document content
        :return:
        '''
        return sitemap_parser.parse_sitemap_xml(sitemap)

    def insert_urls_into_frontier(self,url_list,depth):
        if len(url_list)>0:
            cursor=self.cursor
            page_insert_statement ="""WITH urls(u) 
                              AS (VALUES """+','.join(['(%s)' for i in range(len(url_list))])+""")
                              INSERT INTO crawldb.page(url)
                              (SELECT u FROM urls  
                              WHERE u NOT IN (
                                SELECT url from crawldb.page))
                              RETURNING id;                     
            """
            page_insert_values=tuple(url_list)
            cursor.execute(page_insert_statement, page_insert_values)
            pages_ids=cursor.fetchall()
            if len(pages_ids)>0:
                insert_statement = """INSERT INTO crawldb.frontier(id,depth,status) VALUES """ + ','.join(
                    ["("+str(id[0])+","+str(depth)+",'waiting')" for id in pages_ids]) + ';'
                cursor.execute(insert_statement)



    def handle_robots_sitemaps(self,rp,url):
        cursor = self.cursor
        conn = self.db_conn
        parsed_uri = urlparse(rp.url)
        domain_name = parsed_uri.netloc

        sitemaps_urls = rp.sitemaps
        sitemaps = [Crawler_worker.read_page(sitemap) for sitemap in sitemaps_urls]
        sitemaps_hrefs = [Crawler_worker.process_sitemap(sitemap) for sitemap in sitemaps]
        sitemaps_hrefs = set([href for sitemap_hrefs in sitemaps_hrefs for href in sitemap_hrefs])

        sitemap_content='\n'.join(sitemaps)
        robots_content = rp.raw

        ##### if robots and sitemap data not already in DB --> insert#####
        i_r = rp.robots_exists  # robot.txt file exists
        i_s = sitemap_content != ''  # sitemap exists
        insert_statement = """INSERT INTO crawldb.site (domain""" \
                           + (',robots_content' if i_r else '') \
                           + (', sitemap_content' if i_s else '') + """)
                                      SELECT %s""" \
                           + (', %s' if i_r else '') \
                           + (', %s' if i_s else '') \
                           + """WHERE NOT EXISTS (
                                        SELECT 1 FROM crawldb.site
                                        WHERE domain = %s
                                        LIMIT 1);"""
        insert_values = [domain_name]
        if i_r:
            insert_values.append(robots_content)
        if i_s:
            insert_values.append(sitemap_content)
        insert_values.append(domain_name)
        insert_values = tuple(insert_values)
        cursor.execute(insert_statement, insert_values)
        ##### add new urls from sitemaps to  frontier #####
        self.insert_urls_into_frontier(sitemaps_hrefs,self.get_current_depth(url))
        conn.commit()

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

            ##### PROCESS ROBOTS FILE (returns robotparser object) #####
            rp=self.get_robots(current_url)

            ##### HANDLE ROBOTS AND SITEMAP DATA #####
            self.handle_robots_sitemaps(rp,current_url)

            ##### RENDER/DOWNLOAD WEBPAGE #####
            useragent="*"
            req_response_code, content = self.get_page(url=current_url,useragent=useragent)

            ## TODO : SKIP IF PAGE FETCHING WAS UNSUCCESSFUL ( MARK PAGE AS COMPLETE IN DATABASE ? )

            ##### CHECK IF PAGE CONTENT IMPLIES ALREADY PROCESSED PAGE (if it was, mark job as done) #####
            ## TODO : COMPARE PAGE BY URL IN DATABASE AND CONTENT 
            page_hash = self.get_hash(content)
            if self.duplicate_page(page_hash):
                self.remove_URL(current_url)
                continue

            ##### PARSE WEBPAGE AND EXTRACT IMAGES,DOCUMENTS AND HREFS #####
            images_tmp, documents_tmp, hrefs_tmp = self.parse_page(content)
            images += images_tmp
            documents += documents_tmp
            hrefs += hrefs_tmp

            ##### FILTER NON .GOV.SI HREFS #####
            #only hrefs or also images and documents???
            hrefs = [href_url for href_url in hrefs if Crawler_worker.is_gov_url(href_url)]

            ##### FILTER URLS BASED ON ROBOTS FILE #####
            images = [image_url for image_url in images if rp.can_fetch(useragent,image_url)]
            documents = [document_url for document_url in documents if rp.can_fetch(useragent, document_url)]
            hrefs = [href_url for href_url in hrefs if rp.can_fetch(useragent, href_url)]

            ##### NORMALIZE URLS #####
            images = [Crawler_worker.normalize_url(image_url) for image_url in images]
            documents = [Crawler_worker.normalize_url(document_url) for document_url in documents]
            hrefs = [Crawler_worker.normalize_url(href_url) for href_url in hrefs]

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



