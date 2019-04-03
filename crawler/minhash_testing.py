from datasketch import MinHash, LeanMinHash
#datasketch packages @ https://github.com/ekzhu/datasketch
import page_fetcher
import time
import hashlib


def main():

    rcode_1, page_1 = page_fetcher.fetch_page("http://www.e-prostor.gov.si/zbirke-prostorskih-podatkov/zbirka-vrednotenja-nepremicnin/")
    rcode_2, page_2 = page_fetcher.fetch_page("http://www.e-prostor.gov.si/dostop-do-podatkov/dostop-do-podatkov/")

    start_time = time.time()
    m1_128, m2_128 = MinHash(), MinHash()
    print("128 perm hash time: ", time.time() - start_time)

    start_time = time.time()
    m1, m2 = MinHash(num_perm=256), MinHash(num_perm=256)
    print("256 perm hash time: ", time.time() - start_time)


    for d in page_1:
        m1_128.update(d.encode('utf8'))
        m1.update(d.encode('utf8'))
    for d in page_2:
        m2_128.update(d.encode('utf8'))
        m2.update(d.encode('utf8'))

    print("Estimated Jaccard for page_1 and page_2 is", m1_128.jaccard(m2_128))
    print("Estimated Jaccard for page_1 and page_2 is", m1.jaccard(m2))

    s1 = set(page_1)
    s2 = set(page_2)
    actual_jaccard = float(len(s1.intersection(s2)))/float(len(s1.union(s2)))
    print("Actual Jaccard for page_1 and page_2 is", actual_jaccard)




    sha1 = hashlib.sha1(page_1.encode('utf-8'))
    print(sha1.hexdigest())


    sha2 = hashlib.sha1(page_2.encode('utf-8'))
    print(sha2.hexdigest())

    sha1 = hashlib.sha1("That's what she said.".encode('utf-8'))
    print(sha1.hexdigest())
    sha2 = hashlib.sha1("~Michael Scott".encode('utf-8'))
    print(sha2.hexdigest())
    sha3 = hashlib.sha1("That's what she said.".encode('utf-8'))
    print(sha3.hexdigest())



if __name__ == "__main__":
    main()