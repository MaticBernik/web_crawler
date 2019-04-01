import sys
from crawler_worker import Crawler_worker
import psycopg2
import threading
import time
import json

db_connection_info = json.load(open('connect_DB.json'))
DB_HOST=db_connection_info['host']
DB_NAME=db_connection_info['name']
DB_USER=db_connection_info['username']
DB_PASSWORD=db_connection_info['password']
FRONTIER_SEED_URLS=['http://evem.gov.si','http://e-uprava.gov.si','http://podatki.gov.si','http://e-prostor.gov.si','http://mizs.gov.si','http://mddsz.gov.si','http://mz.gov.si','http://uvps.gov.si','http://mf.gov.si']
FRONTIER_URL_PROCESSING_TIMEOUT_SECONDS=60
NR_WORKERS=8

def unblock_frontier_waiting(conn):
    '''
    FOR TESTING
    Make all URLs in frontier to 'waiting' status
    '''
    cursor=conn.cursor()
    update_statement = "UPDATE crawldb.frontier SET processing_start_time=NULL, status='waiting';"
    cursor.execute(update_statement)
    conn.commit()
    cursor.close()


#CONNECT TO DATABASE
conn = psycopg2.connect(host=DB_HOST,database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
print('***DB connected!')
cursor = conn.cursor()
cursor.execute("""SELECT count(table_name) FROM information_schema.tables WHERE table_schema = 'crawldb'""")
tables_in_crawldb = cursor.fetchone()[0]
if tables_in_crawldb==0:
    cursor.execute(open("crawldb.sql", "r").read())
    conn.commit()
#INITIALIZE FRONTIER
cursor.execute("""SELECT count(*) FROM crawldb.page;""")
pages_nr=cursor.fetchone()[0]
cursor.execute("""SELECT count(*) FROM crawldb.frontier;""")
frontier_pages_nr=cursor.fetchone()[0]
if pages_nr==0 and frontier_pages_nr==0:
    print('***Initializing frontier...')
    page_insert='INSERT INTO crawldb.page(url) VALUES '+','.join(["('"+url+"')" for url in FRONTIER_SEED_URLS])+' RETURNING id;'
    cursor.execute(page_insert)
    row_id=cursor.fetchall()
    frontier_insert='INSERT INTO crawldb.frontier(id,depth,status) VALUES '+','.join(["("+str(id[0])+",0,'waiting')" for id in row_id])+';'
    cursor.execute(frontier_insert)
    conn.commit()
    print('...done!')
unblock_frontier_waiting(conn) #REMOVE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#INITIALIZE AND RUN WORKERS
print('***Running workers in seperate threads...')
workers=[Crawler_worker(db_conn=conn,id='WORKER_'+str(i),frontier_seed_urls=FRONTIER_SEED_URLS) for i in range(NR_WORKERS)]
for worker in workers:
    t = threading.Thread(target=worker.run)
    t.start()
print('...done')

#MAIN LOOP
print('***Entering main loop...')
while True:
    #UNBLOCK TIMED-OUT URLs IN FRONTIER
    #later replace with postgres cron job
    update_statement="UPDATE crawldb.frontier SET processing_start_time=NULL, status='waiting' WHERE status='processing' AND processing_start_time < NOW() - INTERVAL '"+str(FRONTIER_URL_PROCESSING_TIMEOUT_SECONDS)+" seconds';"
    cursor.execute(update_statement)
    conn.commit()
    #EXIT WHEN ALL WORKERS ARE DONE
    if all([not worker.is_running() for worker in workers]):
        break
    #time.sleep(FRONTIER_URL_PROCESSING_TIMEOUT_SECONDS)
cursor.close()
conn.close()
print('***...DONE!')