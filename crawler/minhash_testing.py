from datasketch import MinHash, LeanMinHash
#datasketch packages @ https://github.com/ekzhu/datasketch
import page_fetcher
import time



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

    print(m1_128)
    print(type(m1_128))

    lean_m1 = LeanMinHash(m1)
    lean_m2 = LeanMinHash(m2)

    buf = bytearray(lean_m1.bytesize())
    ser_lm1 = lean_m1.serialize(buf)
    print(ser_lm1)


    buf = bytearray(lean_m2.bytesize())
    ser_lm2 = lean_m2.serialize(buf)
    print(ser_lm2)

if __name__ == "__main__":
    main()