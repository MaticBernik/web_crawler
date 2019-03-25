import sys
import crawler_worker


DB_HOST='testni.streznik.org'
DB_NAME='scraper_db'
DB_USER='username'
DB_PASSWORD='password'
FRONTIER_SEED_URLS=['evem.gov.si','e-uprava.gov.si','podatki.gov.si','e-prostor.gov.si','mizs.gov.si','mddsz.gov.si','mz.gov.si','uvps.gov.si','mf.gov.si']
FRONTIER_URL_PROCESSING_TIMEOUT_SECONDS=600
NR_WORKERS=8




conn = psycopg2.connect(host=DB_HOST,database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
#???check if DB exists
#define DB structure if not exists
#if frontier and page tables are empty => add FRONTIER_SEED_URLS entries to frontier table.
#create workers and run them in seperate threads/processes
workers=[crawler_worker(db_conn=conn) for i in range(NR_WORKERS)]

#periodically check blocked/"processing.." URLs in forntier and unblock them if timed-out
if all([not worker.is_running() for worker in workers]):
    sys.exit()
