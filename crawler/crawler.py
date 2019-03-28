import sys
from crawler_worker import Crawler_worker
import psycopg2
import threading
import time


DB_HOST='testni.streznik.org'
DB_NAME='crawldb'
DB_USER='username'
DB_PASSWORD='password'
FRONTIER_SEED_URLS=['evem.gov.si','e-uprava.gov.si','podatki.gov.si','e-prostor.gov.si','mizs.gov.si','mddsz.gov.si','mz.gov.si','uvps.gov.si','mf.gov.si']
FRONTIER_URL_PROCESSING_TIMEOUT_SECONDS=60
NR_WORKERS=8

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
    frontier_insert='INSERT INTO crawldb.frontier(id) VALUES '+','.join(["("+str(id[0])+")" for id in row_id])+';'
    cursor.execute(frontier_insert)
    conn.commit()
    print('...done!')
#INITIALIZE AND RUN WORKERS
print('***Running workers in seperate threads...')
workers=[Crawler_worker(db_conn=conn) for i in range(NR_WORKERS)]
for worker in workers:
    t = threading.Thread(target=worker.run)
    t.start()
print('...done')

#MAIN LOOP
print('***Entering main loop...')
while True:
    #UNBLOCK TIMED-OUT URLs IN FRONTIER
    #later replace with postgres cron job
    update_statement="UPDATE crawldb.frontier SET processing_start_time=NULL WHERE processing_start_time < NOW() - INTERVAL '"+str(FRONTIER_URL_PROCESSING_TIMEOUT_SECONDS)+" seconds';"
    cursor.execute(update_statement)
    conn.commit()
    #EXIT WHEN ALL WORKERS ARE DONE
    if all([not worker.is_running() for worker in workers]):
        break
    time.sleep(5)
cursor.close()
conn.close()
print('***...DONE!')