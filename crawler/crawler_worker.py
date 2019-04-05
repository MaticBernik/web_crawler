# from urltools import normalize
import psycopg2
from url_normalize import url_normalize as normalize
import urllib
from urllib.parse import urlparse
from threading import Lock
import robotparser
import time
from datetime import datetime
import page_fetcher
import sitemap_parser
import page_parser
import ssl
import hashlib
import mimetypes


class Crawler_worker:
    #Lock every entry separately?
    cache_robots={}
    cache_robots_lock=Lock()
    domain_last_accessed={}
    domain_last_accessed_lock=Lock()
    DOMAIN_DEFAULT_MINIMUM_SECONDS_BETWEEN_REQUESTS=0

    def is_running(self):
        return self.running

    def get_next_frontier_job_id(self,domain_priority=True):
        # TRY TO FETCH URL FROM DOMAIN, THAT WAS NOT VISITED/REQUESTED RECENTLY
        if len(Crawler_worker.domain_last_accessed) == 0:
            domain_priority=False
        cursor = self.cursor
        conn = self.db_conn
        domain_priority_order=[(None,None)]
        if domain_priority_order:
            domain_priority_order = sorted(Crawler_worker.domain_last_accessed.items(), key=lambda kv: -kv[1])
            domain_priority_order.append((None,None))
        for domain, time in domain_priority_order:
            # ENSURE BREADTH-FIRST STRATEGY
            select_statement = """SELECT MIN(depth) from crawldb.frontier WHERE status='waiting'"""
            select_statement = """SELECT crawldb.page.id 
                                                    FROM crawldb.frontier INNER JOIN crawldb.page ON crawldb.page.id=crawldb.frontier.id  
                                                    WHERE status = 'waiting' 
                                                        AND processing_start_time IS NULL 
                                                        AND depth = (""" + select_statement + """)"""\
                                                        +("""AND url LIKE %s""" if domain is not None else '')\
                                                    +"""ORDER BY crawldb.frontier.placement FOR SHARE SKIP LOCKED LIMIT 1"""
            update_statement = """UPDATE crawldb.frontier SET processing_start_time='now', status='processing' 
                                                    WHERE id= (""" + select_statement + """)
                                                    RETURNING crawldb.frontier.id;"""
            if domain is not None:
                update_values = ('%' + domain + '%',)
                cursor.execute(update_statement,update_values)
            else:
                cursor.execute(update_statement)
            conn.commit()
            if cursor.rowcount==0:
                continue
            else:
                result = cursor.fetchone()
                next_page=result[0]
                return next_page


    def get_next_URL(self):
        #get next URL from frontier if not empty else None
        #mark URL in frontier as being processed
        #start timer/add timestamp
        cursor=self.cursor
        conn=self.db_conn
        #TRY TO FETCH URL FROM DOMAIN, THAT WAS NOT VISITED/REQUESTED RECENTLY
        next_page_id=self.get_next_frontier_job_id()
        if next_page_id is None:
            return None
        select_statement="""SELECT crawldb.page.id,crawldb.page.url FROM crawldb.page WHERE id="""+str(next_page_id)+';'
        cursor.execute(select_statement)
        next_page=cursor.fetchone()
        print(self.id+': NEXT PAGE: ',next_page)
        return  next_page[1]

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
    def canonicalize_url(url):
        normalized_url = normalize(url)
        parsed_url = urlparse(normalized_url)
        canonical_url = '{uri.scheme}://{uri.netloc}/{uri.path}'.format(uri=parsed_url)
        return normalize(canonical_url)


    @staticmethod
    def normalize_url(url):
        # return normalize(url)
        return Crawler_worker.canonicalize_url(url)

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
                              WHERE url=%s;"""
        select_values=(normalized_url,)
        cursor.execute(select_statement,select_values)
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
        domain = Crawler_worker.remove_www(parsed_uri.netloc)
        domain_url = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        ##### restore from cache if stored else create #####
        if domain in Crawler_worker.cache_robots:
            rp = Crawler_worker.cache_robots[domain]
        else:
            robots_url = domain_url + 'robots.txt'
            rp = robotparser.RobotFileParser()
            rp.set_url(robots_url)
            try:
                rp.read()
            except Exception as e:
                print(self.id,'EXCEPTION get_robots()',e)
                pass

        Crawler_worker.cache_robots_lock.acquire()
        self.cache_robots_lock_timestamp = time.time()
        if domain not in Crawler_worker.cache_robots:
            Crawler_worker.cache_robots[domain] = rp
        Crawler_worker.cache_robots_lock.release()
        self.cache_robots_lock_timestamp = None
        return rp

    @staticmethod
    def read_page(url):
        """Reads the URL and feeds it to the parser."""
        """Copied and adapted from robotparser.py"""
        try:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            f = urllib.request.urlopen(url, context=ctx)
        except urllib.error.HTTPError as err:
            if err.code in (401, 403):
                #Forbidden,unauthorized
                pass
            elif err.code >= 400 and err.code < 500:
                #retry?
                pass
        else:
            try:
                raw = f.read()
                raw = raw.decode("utf-8")
            except UnicodeDecodeError as e:
                raw = str(raw)

            return raw

    def get_page(self,url,useragent):
        response_code, page_html = page_fetcher.fetch_page(url, self.id)
        return response_code, page_html

    def get_hash(self,content):
        try:
            sha = hashlib.sha1(content.encode('utf-8'))
            return sha.hexdigest()
        except:
            return None

    def get_content_type(content):
        #check if img/document/html...
        pass

    @staticmethod
    def remove_www(domain):
        if 'www.' in domain:
            return domain[domain.index('www.')+4:]
        else:
            return domain

    def get_site_id(self,domain):
        select_statement="""SELECT id
                            FROM crawldb.site
                            WHERE crawldb.site.domain = %s;"""
        select_values=(domain,)
        self.cursor.execute(select_statement,select_values)
        site_id=self.cursor.fetchone()
        site_id=site_id[0] if len(site_id)>0 else None
        return site_id

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
        #Update 1 DataFrame instead of many dicts!!

        cursor = self.cursor
        #get current page id
        self.state=("SAVING DATA TO DB - getting current page id",time.time())
        select_statement = 'SELECT id from crawldb.page where url = %s;'
        select_values = (data['url'],)
        cursor.execute(select_statement,select_values)
        current_page_id = cursor.fetchone()[0]
        #get current domain site id
        self.state = ("SAVING DATA TO DB - getting current domain site id", time.time())
        select_statement = 'SELECT id from crawldb.site where domain = %s;'
        select_values = (data['domain'],)
        cursor.execute(select_statement, select_values)
        current_site_id = cursor.fetchone()[0]
        #fill out current page data record in DB
        self.state = ("SAVING DATA TO DB - filling out current page data", time.time())
        page_type_code = 'DUPLICATE' if data['is_duplicate'] else 'HTML'
        update_statement = 'UPDATE crawldb.page SET '+\
                           'site_id = %s '+\
                           ',accessed_time = %s '+ \
                           ',page_type_code = %s '+ \
                           (',minhash = %s ' if data['minhash'] is not None else '')+\
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
        self.state = ("SAVING DATA TO DB - inserting hrefs into frontier", time.time())
        hrefs_pages_id=self.insert_urls_into_frontier(data['hrefs_urls'],data['depth']+1)
        #insert into link table
        self.state = ("SAVING DATA TO DB - inserting links", time.time())
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
        self.db_conn.commit()
        ##### PROCESS IMAGES #####
        self.state = ("SAVING DATA TO DB - processing images", time.time())
        # filter out image urls longer than 3000 char
        data['image_urls'] = [url for url in data['image_urls'] if len(url) <= 3000]
        if len(data['image_urls'])>0:
            # insert pages for images
            self.state = ("SAVING DATA TO DB - inserting image pages", time.time())
            self.insert_urls_into_pages(data['image_urls'])
            image_id_url=self.urls2pages_ids(data['image_urls'])
            #insert sites for images
            self.state = ("SAVING DATA TO DB - inserting image sites", time.time())
            image_id_domain={id:Crawler_worker.remove_www(urlparse(url).netloc) for url,id in image_id_url.items()}
            domain_site_id={}
            for image_domain in  set(image_id_domain.values()):
                if not image_domain:
                    continue
                rp=self.get_robots('http://'+image_domain)
                self.handle_robots_sitemaps(rp,data['depth']+1)
                site_id=self.get_site_id(image_domain)
                domain_site_id[image_domain]=site_id
            #fill out page data for images
            self.state = ("SAVING DATA TO DB - filling out image pages", time.time())
            for image_page_url,image_page_id in image_id_url.items():
                if not image_id_domain[image_page_id]:
                    continue
                update_statement = 'UPDATE crawldb.page SET ' + \
                                   'site_id = %s ' + \
                                   ',accessed_time = %s ' + \
                                   ',page_type_code = %s ' + \
                                   (',http_status_code = %s ' \
                                        if image_page_url in data['images_content'] \
                                           and data['images_content'][image_page_url][0] is not None else '') + \
                                   'WHERE id = %s;'
                update_values = [domain_site_id[image_id_domain[image_page_id]], datetime.now(), 'BINARY']
                if image_page_url in data['images_content'] and data['images_content'][image_page_url][0] is not None:
                    update_values.append(data['images_content'][image_page_url][0])
                update_values.append(image_page_id)
                update_values = tuple(update_values)
                cursor.execute(update_statement, update_values)
            self.db_conn.commit()
            # insert into image table
            self.state = ("SAVING DATA TO DB - inserting into image table", time.time())
            for image_url, (http_code, image_content) in data['images_content'].items():
                image_page_id = [id for id, url in image_id_url.items() if url == image_url]
                if len(image_page_id)==0:
                    continue
                else:
                    image_page_id=image_page_id[0]
                content_type=image_url[image_url.rfind('.')+1:].upper()
                if len(content_type)>=4:
                    if "PNG" in content_type.upper():
                        content_type="PNG"
                    elif "JPG" in content_type.upper():
                        content_type="JPG"
                    elif "GIF" in content_type.upper():
                        content_type="GIF"
                    elif 'IMAGE/' in content_type:
                        start_idx=content_type.index('IMAGE/')+len('IMAGE/')
                        content_type=content_type[start_idx:start_idx+3]
                if len(content_type)>50:
                    continue
                file_name=image_url[image_url.rfind('/')+1:]
                insert_statement='INSERT INTO crawldb.image(page_id,filename,content_type,data,accessed_time) VALUES(%s,%s,%s,%s,%s);'
                insert_values=(image_page_id,file_name,content_type,image_content,datetime.now())
                cursor.execute(insert_statement,insert_values)
            self.db_conn.commit()
        ##### PROCESS DOCUMENTS #####
            self.state = ("SAVING DATA TO DB - processing documents", time.time())
            # filter out document urls longer than 3000 char
            data['document_urls'] = [url for url in data['document_urls'] if len(url) <= 3000]
            if len(data['document_urls']) >0:
                # insert pages for documents
                self.state = ("SAVING DATA TO DB - inserting document pages", time.time())
                self.insert_urls_into_pages(data['document_urls'])
                document_id_url=self.urls2pages_ids(data['document_urls'])
                # insert sites for documents
                self.state = ("SAVING DATA TO DB - inserting document sites", time.time())
                document_id_domain = {id: Crawler_worker.remove_www(urlparse(url).netloc) for url,id in document_id_url.items()}
                domain_site_id = {}
                for document_domain in set(document_id_domain.values()):
                    if not document_domain:
                        continue
                    rp = self.get_robots('http://' + document_domain)
                    self.handle_robots_sitemaps(rp, data['depth'] + 1)
                    site_id = self.get_site_id(document_domain)
                    domain_site_id[document_domain] = site_id
                # fill out page data for documents
                self.state = ("SAVING DATA TO DB - filling out document pages", time.time())
                for document_page_url, document_page_id in document_id_url.items():
                    if not document_id_domain[document_page_id]:
                        continue
                    update_statement = 'UPDATE crawldb.page SET ' + \
                                       'site_id = %s ' + \
                                       ',accessed_time = %s ' + \
                                       ',page_type_code = %s ' + \
                                       (',http_status_code = %s ' \
                                            if document_page_url in data['documents_content'] \
                                               and data['documents_content'][document_page_url][
                                                   0] is not None else '') + \
                                       'WHERE id = %s;'
                    update_values = [domain_site_id[document_id_domain[document_page_id]], datetime.now(), 'BINARY']
                    if document_page_url in data['documents_content'] and \
                            data['documents_content'][document_page_url][0] is not None:
                        update_values.append(data['documents_content'][document_page_url][0])
                    update_values.append(document_page_id)
                    update_values = tuple(update_values)
                    cursor.execute(update_statement, update_values)
                self.db_conn.commit()
                self.state = ("SAVING DATA TO DB - inserting into page_data", time.time())
                for document_url, (http_code, document_content) in data['documents_content'].items():
                    document_page_id = [id for id, url in document_id_url.items() if url == document_url]
                    if len(document_page_id) == 0:
                        continue
                    else:
                        document_page_id = document_page_id[0]
                    document_data_type=self.get_data_type(document_url)
                    insert_statement="""INSERT INTO crawldb.page_data(page_id,data_type_code,data) VALUES (%s,%s,%s);"""
                    insert_values=(document_page_id,document_data_type,document_content)
                    cursor.execute(insert_statement,insert_values)
        self.state = ("SAVING DATA TO DB - committing changes", time.time())
        self.db_conn.commit()

    def duplicate_page(self,page_hash):
        if page_hash is None:
            return False
        #check if page with specified page_hash is already in DB
        cursor=self.cursor
        select_statement = """SELECT id FROM crawldb.page 
                                                WHERE minhash = %s LIMIT 1;"""
        select_values=(str(page_hash),)
        cursor.execute(select_statement,select_values)
        id = cursor.fetchone()
        if id is None:
            return False
        else:
            return id[0]


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
        '''
        select_statement = """SELECT count(*) 
                              FROM crawldb.page
                              WHERE page_type_code='HTML and ';"""
        '''
        select_statement = """SELECT count(*) 
                              FROM crawldb.site INNER JOIN 
                              (SELECT id,site_id FROM crawldb.page WHERE site_id IS NOT NULL AND page_type_code='HTML') as pages_with_site
                              ON crawldb.site.id = pages_with_site.site_id
                              WHERE domain LIKE '%gov.si%';"""
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

    def insert_urls_into_pages(self,url_list):
        conn = self.db_conn
        cursor = self.cursor
        if len(url_list)>0:
            #insert pages for URLs not already stored in crawldb.pages
            page_insert_statement = """WITH urls(u) 
                                          AS (VALUES """ + ','.join(['(%s)' for i in range(len(url_list))]) + """)
                                          INSERT INTO crawldb.page(url)
                                          (SELECT u FROM urls  
                                          WHERE u NOT IN (
                                            SELECT url from crawldb.page));                     
                                            """
            page_insert_values = tuple(url_list)
            cursor.execute(page_insert_statement, page_insert_values)
            conn.commit()

    def urls2pages_ids(self,url_list):
        conn = self.db_conn
        cursor = self.cursor
        result={}
        if len(url_list)>0:
            #Retrieve id's of pages with URLs listed in URL list
            select_statement="""WITH urls(u) 
                              AS (VALUES """+','.join(['(%s)' for i in range(len(url_list))])+""")
                              SELECT url,id from crawldb.page
                              WHERE EXISTS (SELECT 1 FROM urls WHERE urls.u=url);"""
            select_values=tuple(url_list)
            cursor.execute(select_statement,select_values)
            pages_urls_ids = cursor.fetchall()
            result={page[0]:page[1] for page in pages_urls_ids}
        return result


    def insert_urls_into_frontier(self,url_list,depth):
        ##### MAKE SURE THAT INSERTED URLS ARE NOT ALREADY IN FRONTIER OR FILES..
        ##### FILTER NON .GOV.SI HREFS #####
        #only hrefs or also images and documents???
        url_list = [href_url for href_url in url_list if Crawler_worker.is_gov_url(href_url)]
        ##### FILTER: HREFS MUST NOT BE '.' #####
        url_list = [href_url for href_url in url_list if not href_url.strip() == '.']
        ##### FILTER: HREFS MUST NOT POINT TO A FILE!!!!!! #####
        #url_list = [href_url for href_url in url_list if not Crawler_worker.is_file_url(href_url)]
        url_list = list(set(url_list))

        # First insert pages for URLs not already stored in crawldb.pages
        self.insert_urls_into_pages(url_list)

        if len(url_list)>0:
            conn=self.db_conn
            cursor=self.cursor
            #Retrieve id's of pages with URLs listed in URL list
            pages_ids=self.urls2pages_ids(url_list).values()
            #For every retrieved page create corresponding frontier entry if not exists
            insert_statement="""WITH pages(id) 
                              AS (VALUES """+','.join(['(%s)' for i in range(len(pages_ids))])+""")
                              INSERT INTO crawldb.frontier(id,status,depth)
                              SELECT pages.id,'waiting',"""+str(depth)+""" FROM pages
                              WHERE NOT EXISTS (SELECT 1 FROM crawldb.frontier WHERE pages.id=crawldb.frontier.id);
                              """
            insert_values = tuple(pages_ids)
            cursor.execute(insert_statement,insert_values)
            conn.commit()




    def handle_robots_sitemaps(self,rp,current_depth):
        cursor = self.cursor
        conn = self.db_conn
        parsed_uri = urlparse(rp.url)
        domain = Crawler_worker.remove_www(parsed_uri.netloc)

        self.state=("HANDLING SITEMAP - processing hrefs",time.time())
        sitemaps_urls = rp.sitemaps
        sitemaps = [Crawler_worker.read_page(sitemap) for sitemap in sitemaps_urls]
        sitemaps = [sitemap for sitemap in sitemaps if sitemap is not None]
        sitemaps_hrefs = [Crawler_worker.process_sitemap(sitemap) for sitemap in sitemaps]
        sitemaps_hrefs = set([href for sitemap_hrefs in sitemaps_hrefs for href in sitemap_hrefs])
        ##### FILTER URLS BASED ON ROBOTS FILE #####
        sitemaps_hrefs = [href_url for href_url in sitemaps_hrefs if rp.can_fetch('*', href_url)]

        sitemap_content='\n'.join(sitemaps)
        robots_content = rp.raw

        ##### if robots and sitemap data not already in DB --> insert#####
        i_r = rp.robots_exists  # robot.txt file exists
        i_s = sitemap_content != ''  # sitemap exists

        insert_statement = """INSERT INTO crawldb.site (domain""" \
                           + (',robots_content' if i_r else '') \
                           + (',sitemap_content' if i_s else '') + """)
                                    SELECT %s""" + (',%s' if i_r else '') + (',%s' if i_s else '') + """
                                    WHERE NOT EXISTS (
                                                SELECT 1 FROM crawldb.site
                                                WHERE domain = %s
                                                LIMIT 1);"""

        insert_values = [domain]
        if i_r:
            insert_values.append(robots_content)
        if i_s:
            insert_values.append(sitemap_content)
        insert_values.append(domain)
        insert_values = tuple(insert_values)
        cursor.execute(insert_statement, insert_values)
        conn.commit()

                ##### add new urls from sitemaps to  frontier #####
        self.state=("HANDLING SITEMAP - inserting into frontier", time.time())
        self.insert_urls_into_frontier(sitemaps_hrefs,current_depth+1)
        self.state=("HANDLING SITEMAP - commiting data", time.time())
        conn.commit()


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
        if cached_domain_robots.default_entry is None:
            cached_domain_robots=None
        domain_crawl_delay = cached_domain_robots.crawl_delay('*') if cached_domain_robots is not None and cached_domain_robots.crawl_delay('*') is not None \
                              else Crawler_worker.DOMAIN_DEFAULT_MINIMUM_SECONDS_BETWEEN_REQUESTS
        Crawler_worker.domain_last_accessed_lock.acquire()
        try:
            if domain in Crawler_worker.domain_last_accessed:
                if time.time() < Crawler_worker.domain_last_accessed[domain] + domain_crawl_delay:
                    # just sleep and wait-out the remaining time?
                    Crawler_worker.domain_last_accessed_lock.release()
                    return True
            Crawler_worker.domain_last_accessed[domain] = time.time()
        except Exception as e:
            print(self.id,'domain_locked() EXCEPTION!!!',e)
        Crawler_worker.domain_last_accessed_lock.release()
        return False

    @staticmethod
    def dowload_binary(url,actually_wanna_download_big_and_slow_files=False):
        #dowload binary data (image or document)
        #Return tuple of form (http_status_code,content)
        if not actually_wanna_download_big_and_slow_files:
            return 404, None
        current_domain = Crawler_worker.remove_www(urlparse(url).netloc)
        while Crawler_worker.domain_locked(current_domain):
            pass
        response_code, binary_file = page_parser.fetch_file_content(url)
        return response_code, binary_file

    @staticmethod
    def guess_type_of(link, strict=True):
        """ DO NOT USE - CAUSING EXCEPTIONS """
        link_type, _ = mimetypes.guess_type(link)
        if link_type is None and strict:
            with urllib.request.urlopen(link) as u:
                link_type = u.info().get_content_type()  # or using: u.info().gettype()
        return link_type

    @staticmethod
    def is_file_url(url):
        url_ending=url[-6:] if len(url)>=6 else url
        url_ending=url_ending.lower()
        if '.htm' in url_ending or '.html' in url_ending or '.xhtml' in url_ending or url_ending[-1]=='/':
            return False
        file_suffixes=['.evem','.edit','.swf','jspx','.zip','.mp4','.mp3','.jpg','.jpeg','.png','.vaw','.vma','.aspx',\
                       '.doc','.pdf','.docx','.ppt','.xlsx','.xls','.xsd','.jsp','.txt','.xml','.ppsm','.ppsx','.aif'\
                       '.cda','.mid','.midi','.mpa','.wav','.vma','.wpl','.7z','.arj','.deb','.pkg','.rar','.rpm','.tar.gz'\
                       '.z','bin','.exe','.iso','.csv','.dat','.db','.dbf','.log','.mdf','.sql','.tar','.apk','.bat',\
                       '.exe','.jar','.wsf','.py','.ttf','.otf','.fnt','.fon','.bmp','.ico','.ps','.svg','.tif','.tiff',\
                       '.asp','.cer','.css','.cgi','.pl','.js','.jsp','.part','.php','.rss','.key','.odp','.pps','.ppt',\
                       '.pps','.pptx','.ods','.xlr','.vb','.tmp','.ico','.msi','.dll','.cab','.bak','.avi','.3g2','.3gp',\
                       '.h264','.mkv','.wmv','.vob','.rm','.flv','.rtf','.tex','.wps','.odt']
        for suffix in file_suffixes:
            if suffix in url_ending:
                return True
        '''
        if '.' in url_ending:
            suffix=url_ending[url_ending.index('.'):]
            print("**** Suffix ",suffix,'found in HREF URL...... IS THAT OK??',url)
        '''
        '''
        if not 'text' in Crawler_worker.guess_type_of(url):
            return True
        '''
        if not page_fetcher.is_text_html(url):
            return True

        return False

    def update_page_early_stop(self,url,domain,http_status_code):
        ###ADD http_status_code, site_id AND accessed_time TO PAGE
        cursor=self.cursor
        conn=self.db_conn
        select_statement="SELECT id "+ \
                         "FROM crawldb.site WHERE domain = %s;"
        select_values=(domain,)
        cursor.execute(select_statement,select_values)
        site_id=cursor.fetchone()[0]
        update_statement="UPDATE crawldb.page"+ \
                         "  SET site_id = %s, "+ \
                         "      http_status_code = %s, "+\
                         "      accessed_time = %s, "+ \
                         "      page_type_code = 'HTML'"+\
                         "  WHERE url = %s;"
        update_values=(site_id,http_status_code,datetime.now(),url)
        cursor.execute(update_statement,update_values)
        conn.commit()

    def run_logic(self):
        while True:
            #print(self.id,"GETTING NEW JOB")
            images = []
            documents = []
            hrefs = []
            ##### CHECK IF 100.000 WEB PAGES ALREADY PROCESSED #####
            if self.early_stop_condition():
                print(self.id+'EARLY-STOP CONDITION REACHED ...exiting!')
                break
            ##### TRY TO GET NEXT JOB/URL (exit after 5 retries) #####
            self.state=("GETTIN URL JOB",time.time())
            for retry in range(5):
                current_url = self.get_next_URL()
                if current_url is not None:
                    break
                else:
                    print(self.id+' without URL job...retrying in 5s...')
                    time.sleep(5)
            else:
                break

            #print('\t',self.id, "CHECKING IF NEW URL ALREADY PROCESSED")
            ##### CHECK IF NEW JOB/URL WAS ALREADY PROCESSED (if it was, mark job as done) #####
            if self.url_already_processed(current_url):
                print("ALREADY PROCESSED")
                self.processing_done_URL(current_url)
                continue

            #print('\t',self.id, "POCESSING ROBOTS FILE")
            ##### PROCESS ROBOTS FILE (returns robotparser object) #####
            self.state=("PROCESSING ROBOTS",time.time())
            rp=self.get_robots(current_url)

            #print('\t',self.id, "GETTING CURRENT DEPTH AND DOMAIN")
            ##### GET CURRENT DEPTH AND DOMAIN #####
            current_depth = self.get_current_depth(current_url)
            current_domain = Crawler_worker.remove_www(urlparse(current_url).netloc)

            #print('\t',self.id, "HANDLING ROBOTS AND SITEMAP DATA FOR DOMAIN")
            ##### HANDLE ROBOTS AND SITEMAP DATA #####
            self.state=('HANDLING SITEMAP',time.time())
            self.handle_robots_sitemaps(rp,current_depth)

            #print('\t',self.id, "RENDERING AND DOWNLOADING WEBPAGE")
            ##### RENDER/DOWNLOAD WEBPAGE #####
            # self.state=("RENDERING WEBPAGE - before domain locked",time.time())
            useragent="*"
            # while Crawler_worker.domain_locked(current_domain):
                # pass
            self.state=("RENDERING WEBPAGE",time.time())
            req_response_code, content = self.get_page(url=current_url,useragent=useragent)

            if content is None:
                print(self.id, "DOWNLOADED CONTENT IS NONE... JOB DONE:D")
                ###ADD http_status_code, site_id AND accessed_time TO PAGE
                self.update_page_early_stop(current_url,current_domain,req_response_code)
                self.processing_done_URL(current_url)
                continue

            #print('\t',self.id, "HASHING AND CHECKING FOR DUPLICATE")
            ##### CHECK IF PAGE CONTENT IMPLIES ALREADY PROCESSED PAGE (if it was, mark job as done) #####
            ## TODO : COMPARE PAGE BY CONTENT
            self.state=("HASHING",time.time())
            page_hash = self.get_hash(content)
            is_duplicate = self.duplicate_page(page_hash)

            #print('\t',self.id, "PARSING WEBPAGE")
            ##### PARSE WEBPAGE AND EXTRACT IMAGES,DOCUMENTS AND HREFS #####
            # self.state=('PARSING WEBPAGE - waiting domain lock',time.time())
            # while Crawler_worker.domain_locked(current_domain):
                # pass
            self.state=('PARSING WEBPAGE',time.time())
            images_tmp, documents_tmp, hrefs_tmp = self.parse_page(current_url, content)
            images += images_tmp
            documents += documents_tmp
            hrefs += hrefs_tmp

            #print('\t',self.id, "NORMALIZING EXTRACTED URLS")
            self.state=("NORMALIZING EXTRACTED URLs",time.time())
            ##### NORMALIZE URLS #####
            images = [Crawler_worker.normalize_url(image_url) for image_url in images]
            documents = [Crawler_worker.normalize_url(document_url) for document_url in documents]
            hrefs = [Crawler_worker.normalize_url(href_url) for href_url in hrefs]

            ##### MAKE SURE URLS ARE UNIQUE #####
            images=set(images)
            documents=set(documents)
            hrefs=set(hrefs)

            ##### FILTER URLS ALREADY PROCESSED #####
            self.state=("FILTERING ALREADY PROCESSED EXTRACTED URLs",time.time())
            images = [image_url for image_url in images if not self.url_in_frontier(image_url)]
            documents = [document_url for document_url in documents if not self.url_in_frontier(document_url)]
            hrefs = [href_url for href_url in hrefs if not self.url_in_frontier(href_url)]

            ##### FILTER NON .GOV.SI HREFS #####
            #only hrefs or also images and documents???
            hrefs = [href_url for href_url in hrefs if Crawler_worker.is_gov_url(href_url)]

            ##### FILTER URLS BASED ON ROBOTS FILE #####
            self.state=('ROBOTS FILTERING EXTRACTED URLS (can_fetch)')
            images = [image_url for image_url in images if rp.can_fetch(useragent,image_url)]
            documents = [document_url for document_url in documents if rp.can_fetch(useragent, document_url)]
            hrefs = [href_url for href_url in hrefs if rp.can_fetch(useragent, href_url)]

            ##### FILTER: HREFS MUST NOT BE '.' #####
            hrefs = [href_url for href_url in hrefs if not href_url.strip()=='.']

            ##### FILTER: HREFS MUST NOT POINT TO A FILE!!!!!! #####
            self.state=("FILTERING FILES FROM HREF EXTRACTED URLS",time.time())
            hrefs = [href_url for href_url in hrefs if not Crawler_worker.is_file_url(href_url)]

            ##### COLLECT BINARIES ONLY FROM SITES LISTED IN THE INITIAL SEED LIST #####
            self.state=("DOWNLOADING EXTRACTED BINARIES",time.time())
            images_content={image_url: Crawler_worker.dowload_binary(image_url) for image_url in images if Crawler_worker.remove_www(urlparse(image_url).netloc) in self.frontier_seed_sites}
            documents_content = {document_url: Crawler_worker.dowload_binary(document_url) for document_url in documents if Crawler_worker.remove_www(urlparse(document_url).netloc) in self.frontier_seed_sites}


            ##### GET DOCUMENT DATA_TYPE #####
            self.state=('GETTING EXTRACTED DOCUMENT DATA TYPE',time.time())
            documents_data_type={document_url: self.get_data_type(document_url) for document_url in documents}

            ##### WRITE NEW DATA TO DB IN SINGLE TRANSACTION #####
            #print('\t',self.id,'SAVING DATA TO DB')
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

            self.state = ("WAITING-DOMAIN-LOCKUP", time.time())

            while Crawler_worker.domain_locked(current_domain):
                pass

            self.state = ("SAVING DATA TO DB", time.time())
            self.write_to_DB(data=data)
            #print('\t',self.id,'DATA SAVED')
            self.state=("DATA SAVED",time.time())
            self.processing_done_URL(current_url)

    def run(self):
        print(self.id+' RUNNING..')
        self.running = True
        '''
        while True:
            try:
                self.run_logic()
            except Exception as e:
                print(self.id+' EXCEPTION!!!!!!!! restarting worker..',str(e))
                self.cursor.execute("ROLLBACK;")
                self.db_conn.commit()
            else:
                break;
        '''
        self.run_logic()
        print(self.id+' exiting!')
        self.cursor.close()
        self.running = False

    def __init__(self, db_connection_info, frontier_seed_urls, id='WORKER'):
        DB_HOST = db_connection_info['host']
        DB_NAME = db_connection_info['name']
        DB_USER = db_connection_info['username']
        DB_PASSWORD = db_connection_info['password']
        while True:
            try:
                db_conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
            except:
                continue
            else:
                break
        self.db_conn=db_conn
        self.cursor=db_conn.cursor()
        self.id=id
        self.frontier_seed_sites=[Crawler_worker.remove_www(urlparse(seed_url).netloc) for seed_url in frontier_seed_urls]
        self.cache_robots_lock_timestamp=None
        self.state=('INITIALIZATION',time.time())
        #self.domain_last_accessed_lock_timestamp=None



