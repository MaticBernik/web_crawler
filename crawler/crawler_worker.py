from urltools import normalize
import urllib
from urllib.parse import urlparse
from threading import Lock
import robotparser
import time
from datetime import datetime
import page_fetcher
import sitemap_parser
import page_parser


class Crawler_worker:
    #Lock every entry separately?
    cache_robots={}
    cache_robots_lock=Lock()
    domain_last_accessed={}
    domain_last_accessed_lock=Lock()
    DOMAIN_DEFAULT_MINIMUM_SECONDS_BETWEEN_REQUESTS=2

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
                              WHERE id = (SELECT id FROM crawldb.page WHERE url = %s);"""
        delete_values = (normalized_url,)
        cursor.execute(delete_statement,delete_values)
        conn.commit()
        return True

    def processing_done_URL(self,url):
        cursor = self.cursor
        conn = self.db_conn
        normalized_url = normalize(url)
        update_statement = "UPDATE crawldb.frontier SET status='done' WHERE id = (SELECT id FROM crawldb.page WHERE url = %s LIMIT 1);"
        update_values=(normalized_url,)
        cursor.execute(update_statement,update_values)
        conn.commit()
        return True

    def url_already_processed(self,url,normalize_url=False):
        #check if URL already in column url of table page
        cursor=self.cursor
        if normalize_url:
            normalized_url = Crawler_worker.normalize_url(url)
        else:
            normalized_url=url
        select_statement = """SELECT exists (
                                SELECT 1 FROM crawldb.page INNER JOIN crawldb.frontier ON crawldb.page.id=crawldb.frontier.id  
                                WHERE crawldb.frontier.status='done' AND crawldb.page.url = %s LIMIT 1);"""
        select_values = (normalized_url,)
        cursor.execute(select_statement,select_values)
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
                                    SELECT id from crawldb.page WHERE url = %s) 
                                LIMIT 1);"""
        select_values = (normalized_url,)
        cursor.execute(select_statement, select_values)
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
        domain = parsed_uri.netloc
        domain_url = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        ##### restore from cache if stored else create #####
        Crawler_worker.cache_robots_lock.acquire()
        if domain in Crawler_worker.cache_robots:
            rp=Crawler_worker.cache_robots[domain]
        else:
            robots_url = domain_url + 'robots.txt'
            rp = robotparser.RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            Crawler_worker.cache_robots[domain]=rp
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

    def write_to_DB(self,data):
        #WITHIN SINGLE TRANSACTION!!!
        #write new data to database
        #and remove current_url from frontier
        #for URLs: DEPTH = DEPTH +1
        '''
        data={'url' : current_url,
                  'domain' : current_domain,
                  'depth' : current_depth,
                  'http_status_code' : req_response_code,
                  'html_content' : content,
                  'minhash' : page_hash,
                  'is_duplicate' : is_duplicate,
                  'image_urls' : images,
                  'document_urls' : documents,
                  'hrefs_urls' : hrefs,
                  'images_content' : images_content,
                  'documents_content' : documents_content,
                  'documents_data_type' : documents_data_type
            }
        '''
        cursor = self.cursor
        #get current page id
        select_statement = 'SELECT id from crawldb.page where url = %s;'
        select_values = (data['url'],)
        cursor.execute(select_statement,select_values)
        current_page_id = cursor.fetchone()[0]
        #get current domain site id
        select_statement = 'SELECT id from crawldb.site where domain = %s;'
        select_values = (data['domain'],)
        cursor.execute(select_statement, select_values)
        current_site_id = cursor.fetchone()[0]
        #fill out current page data record in DB
        page_type_code = 'DUPLICATE' if data['is_duplicate'] else 'HTML'
        update_statement = 'UPDATE crawldb.page SET '+\
                           'site_id = %s '+\
                           ',accessed_time = %s '+ \
                           ',page_type_code = %s '+ \
                           ('minhash = %s ' if data['minhash'] is not None else '')+\
                           (',html_content = %s ' if page_type_code!='DUPLICATE' and data['html_content'] is not None else '')+\
                           (',http_status_code = %s ' if data['http_status_code'] is not None else '')+\
                           'WHERE id = %s;'
        update_values = [current_site_id,datetime.now(),page_type_code]
        if data['minhash'] is not None:
            update_values.append(data['minhash'])
        if page_type_code!='DUPLICATE' and data['html_content'] is not None:
            update_values.append(data['html_content'])
        if data['http_status_code'] is not None:
            update_values.append(data['http_status_code'])
        update_values.append(current_page_id)
        update_values=tuple(update_values)
        cursor.execute(update_statement, update_values)
        #insert href urls to frontier and create corresponding page entries
        hrefs_pages_id=self.insert_urls_into_frontier(data['hrefs_urls'],data['depth']+1)
        #insert into link table
        if page_type_code=='DUPLICATE':
            duplicate_page_id=self.duplicate_page(data['minhash'])
            insert_statement='INSERT INTO crawldb.link(from_page,to_page) VALUES (%s,%s);'
            insert_values=(duplicate_page_id,current_page_id)
        if hrefs_pages_id is not None and len(hrefs_pages_id) > 0:
            hrefs_pages_id=[x[0] for x in hrefs_pages_id]
            insert_statement = 'INSERT INTO crawldb.link(from_page,to_page) VALUES '+','.join(['(%s,%s)' for i in range(len(hrefs_pages_id))])+';'
            #insert_values=tuple([(current_page_id,id) for id in hrefs_pages_id])
            insert_values=[]
            for id in hrefs_pages_id:
                insert_values+=[current_page_id,id]
            insert_values=tuple(insert_values)
            cursor.execute(insert_statement,insert_values)

        if len(data['image_urls'])>0:
            # insert pages for images
            insert_statement = """WITH urls(u) 
                                  AS (VALUES """+','.join(['(%s)' for i in range(len(data['image_urls']))])+""")
                                  INSERT INTO crawldb.page(url)
                                  (SELECT u FROM urls  
                                  WHERE u NOT IN (
                                    SELECT url from crawldb.page) FOR UPDATE SKIP LOCKED)
                                  RETURNING id,url;
                               """
            insert_values = tuple(data['image_urls'])
            cursor.execute(insert_statement,insert_values)
            image_pages_ids=cursor.fetchall()
            image_id_url={}
            if len(image_pages_ids)>0:
                image_id_url={x[0]:x[1] for x in image_pages_ids}
            #insert sites for images
            image_id_domain={id:urlparse(url).netloc for id,url in image_id_url.items()}
            domain_site_id={}
            for image_domain in  set(image_id_domain.values()):
                if not image_domain:
                    continue
                rp=self.get_robots('http://'+image_domain)
                site_id=self.handle_robots_sitemaps(rp,data['depth']+1)
                if site_id is None:
                    select_statement='SELECT id FROM crawldb.site WHERE domain=%s;'
                    select_values=(image_domain,)
                    cursor.execute(select_statement,select_values)
                    site_id=cursor.fetchone()
                site_id=site_id[0]
                domain_site_id[image_domain]=site_id



            for image_page_id,image_page_url in image_pages_ids:
                if not image_id_domain[image_page_id]:
                    continue
                update_statement = 'UPDATE crawldb.page SET ' + \
                                   'site_id = %s ' + \
                                   ',accessed_time = %s ' + \
                                   ',page_type_code = %s ' + \
                                   (',http_status_code = %s ' \
                                        if image_id_url[image_page_id] in data['images_content'] \
                                           and data['images_content'][image_id_url[image_page_id]][0] is not None else '') + \
                                   'WHERE id = %s;'
                update_values = [domain_site_id[image_id_domain[image_page_id]], datetime.now(), 'BINARY']
                if image_id_url[image_page_id] in data['images_content'] and data['images_content'][image_id_url[image_page_id]][0] is not None:
                    update_values.append(data['images_content'][image_id_url[image_page_id]][0])
                update_values.append(image_page_id)
                update_values = tuple(update_values)
                print('UPDATE VALUES',update_values)
                cursor.execute(update_statement, update_values)




    def duplicate_page(self,page_hash):
        if page_hash is None:
            return False
        #check if page with specified page_hash is already in DB
        cursor=self.cursor
        '''
        select_statement = """SELECT EXISTS (
                                        SELECT 1 FROM crawldb.page 
                                        WHERE minhash = %s LIMIT 1);"""
        '''
        select_statement = """SELECT id FROM crawldb.page 
                                                WHERE minhash = %s LIMIT 1);"""
        select_values=(str(page_hash),)
        cursor.execute(select_statement,select_values)
        already_exists = cursor.fetchone()[0]
        return already_exists


    def parse_page(self, url, content):
        #parse html page and return three lists:
        #list of images, list of hrefs and list of documents
        #each list contains URLs (or tuples??)
        images=[]
        documents=[]
        hrefs=[]

        images, documents, hrefs = page_parser.parse_page_html(url, content)

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
            url_list = list(set(url_list))

            page_insert_statement ="""WITH urls(u) 
                              AS (VALUES """+','.join(['(%s)' for i in range(len(url_list))])+""")
                              INSERT INTO crawldb.page(url)
                              (SELECT u FROM urls  
                              WHERE u NOT IN (
                                SELECT url from crawldb.page) FOR UPDATE SKIP LOCKED)
                              RETURNING id;                     
            """

            '''
            page_insert_statement = """WITH urls(u) 
                              AS (VALUES """+','.join(['(%s)' for i in range(len(url_list))])+""")
                              INSERT INTO crawldb.page(url)
                              SELECT urls.u
                              FROM urls
                              WHERE NOT EXISTS (
                              SELECT 1 FROM crawldb.page WHERE crawldb.page.url = urls.u)
                              FOR UPDATE SKIP LOCKED
                              RETURNING crawldb.page.id; """
            '''


            page_insert_values=tuple(url_list)
            cursor.execute(page_insert_statement, page_insert_values)
            pages_ids=cursor.fetchall()
            if len(pages_ids)>0:
                insert_statement = """INSERT INTO crawldb.frontier(id,depth,status) VALUES """ + ','.join(
                    ["("+str(id[0])+","+str(depth)+",'waiting')" for id in pages_ids]) + ';'
                cursor.execute(insert_statement)
            return pages_ids



    def handle_robots_sitemaps(self,rp,current_depth):
        cursor = self.cursor
        conn = self.db_conn
        parsed_uri = urlparse(rp.url)
        domain = parsed_uri.netloc

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
                                        LIMIT 1) RETURNING id;"""
        insert_values = [domain]
        if i_r:
            insert_values.append(robots_content)
        if i_s:
            insert_values.append(sitemap_content)
        insert_values.append(domain)
        insert_values = tuple(insert_values)
        cursor.execute(insert_statement, insert_values)
        site_id=cursor.fetchone()
        ##### add new urls from sitemaps to  frontier #####
        self.insert_urls_into_frontier(sitemaps_hrefs,current_depth+1)
        conn.commit()
        return site_id

    def get_data_type(self,url):
        #return data type of binary from url
        #data types in database to choose from: 'PDF', 'DOC', 'DOCX', 'PPT', 'PPTX'
        if url.endswith('.pdf') or url.endswith('.PDF'):
            return 'PDF'
        if url.endswith('.doc') or url.endswith('.DOC'):
            return 'DOC'
        if url.endswith('.docx') or url.endswith('.DOCX'):
            return 'DOCX'
        if url.endswith('.ppt') or url.endswith('.PPT'):
            return 'PPT'
        if url.endswith('.pptx') or url.endswith('.pptx'):
            return 'PPTX'
        
        return None

    @staticmethod
    def domain_locked(domain):
        '''
        Check if domain is ready to receive another request respecting crawl delay
        '''
        #USE QUEUE!!!
        cached_domain_robots = Crawler_worker.cache_robots[domain]
        domain_crawl_delay = cached_domain_robots.request_rate[1] if cached_domain_robots.request_rate is not None \
                              else Crawler_worker.DOMAIN_DEFAULT_MINIMUM_SECONDS_BETWEEN_REQUESTS
        Crawler_worker.domain_last_accessed_lock.acquire()
        if domain_url in Crawler_worker.domain_last_accessed:
            if time.time() < Crawler_worker.domain_last_accessed[domain] + domain_crawl_delay:
                # just sleep and wait-out the remaining time?
                Crawler_worker.domain_last_accessed_lock.release()
                return True
        Crawler_worker.domain_last_accessed[domain] = time.time()
        Crawler_worker.domain_last_accessed_lock.release()
        return False

    @staticmethod
    def dowload_binary(url):
        #dowload binary data (image or document)
        #Return tuple of form (http_status_code,content)
        response_code, binary_file = page_parser.fetch_file_content(url)
        return response_code, binary_file



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

            ##### GET CURRENT DEPTH AND DOMAIN #####
            current_depth = self.get_current_depth(current_url)
            current_domain = urlparse(current_url).netloc

            ##### HANDLE ROBOTS AND SITEMAP DATA #####
            self.handle_robots_sitemaps(rp,current_depth)

            ##### RENDER/DOWNLOAD WEBPAGE #####
            useragent="*"
            req_response_code, content = self.get_page(url=current_url,useragent=useragent)

            if content is None:
                self.processing_done_URL(current_url)
                continue

            ##### CHECK IF PAGE CONTENT IMPLIES ALREADY PROCESSED PAGE (if it was, mark job as done) #####
            ## TODO : COMPARE PAGE BY CONTENT
            page_hash = self.get_hash(content)
            is_duplicate = self.duplicate_page(page_hash)


            ##### PARSE WEBPAGE AND EXTRACT IMAGES,DOCUMENTS AND HREFS #####
            images_tmp, documents_tmp, hrefs_tmp = self.parse_page(current_url, content)
            images += images_tmp
            documents += documents_tmp
            hrefs += hrefs_tmp

            ##### NORMALIZE URLS #####
            images = [Crawler_worker.normalize_url(image_url) for image_url in images]
            documents = [Crawler_worker.normalize_url(document_url) for document_url in documents]
            hrefs = [Crawler_worker.normalize_url(href_url) for href_url in hrefs]

            ##### MAKE SURE URLS ARE UNIQUE #####
            images=set(images)
            documents=set(documents)
            hrefs=set(hrefs)

            ##### FILTER URLS ALREADY PROCESSED #####
            images = [Crawler_worker.normalize_url(image_url) for image_url in images if not self.url_already_processed(image_url)]
            documents = [Crawler_worker.normalize_url(document_url) for document_url in documents if not self.url_already_processed(document_url)]
            hrefs = [Crawler_worker.normalize_url(href_url) for href_url in hrefs if not self.url_already_processed(href_url)]

            ##### FILTER NON .GOV.SI HREFS #####
            #only hrefs or also images and documents???
            hrefs = [href_url for href_url in hrefs if Crawler_worker.is_gov_url(href_url)]

            ##### FILTER URLS BASED ON ROBOTS FILE #####
            images = [image_url for image_url in images if rp.can_fetch(useragent,image_url)]
            documents = [document_url for document_url in documents if rp.can_fetch(useragent, document_url)]
            hrefs = [href_url for href_url in hrefs if rp.can_fetch(useragent, href_url)]

            ##### COLLECT BINARIES ONLY FROM SITES LISTED IN THE INITIAL SEED LIST #####
            images_content={image_url: Crawler_worker.dowload_binary(image_url) for image_url in images if image_url in self.frontier_seed_urls}
            documents_content = {document_url: Crawler_worker.dowload_binary(document_url) for document_url in documents if document_url in self.frontier_seed_urls}

            ##### GET DOCUMENT DATA_TYPE #####
            documents_data_type={document_url: self.get_data_type(document_url) for document_url in documents}

            ##### WRITE NEW DATA TO DB IN SINGLE TRANSACTION #####
            data={'url' : current_url,
                  'domain' : current_domain,
                  'depth' : current_depth,
                  'http_status_code' : req_response_code,
                  'html_content' : content,
                  'minhash' : page_hash,
                  'is_duplicate' : is_duplicate,
                  'image_urls' : images,
                  'document_urls' : documents,
                  'hrefs_urls' : hrefs,
                  'images_content' : images_content,
                  'documents_content' : documents_content,
                  'documents_data_type' : documents_data_type
            }
            self.write_to_DB(data=data)
            self.processing_done_URL(current_url)
        print(self.id+' exiting!')
        self.cursor.close()
        self.running = False

    def __init__(self, db_conn, frontier_seed_urls, id='WORKER'):
        self.db_conn=db_conn
        self.cursor=db_conn.cursor()
        self.id=id
        self.frontier_seed_urls=frontier_seed_urls



