import logging
from datasketch import MinHashLSHEnsemble, MinHash

logging.basicConfig(filename='test.log',level=logging.DEBUG)

set1 = set(["cat", "dog", "fish", "cow","carlo","paride","padre","plane","frest"])
set2 = set(["cat", "dog", "fish", "cow", "pig", "elephant", "lion", "tiger",
             "wolf", "bird", "human","rt","rtrtr","tren","frest"])
set3 = set(["cat", "dog", "car", "van", "train", "plane", "ship", "submarine",
             "rocket", "bike", "scooter", "motorcyle", "SUV", "jet", "horse","carlo","cow","paride","padre","plane"])

# Create MinHash objects
m1 = MinHash(num_perm=128)
m2 = MinHash(num_perm=128)
m3 = MinHash(num_perm=128)
#print(m2.hashvalues)
i=0;
for d in set1:
    m1.update(d.encode('utf8'))
    i=i+1
    #print(len(m1.hashvalues))
    #print(m1.hashvalues)
    #print(m1.hashvalues[0])
    #print(i)
for d in set2:
    m2.update(d.encode('utf8'))
for d in set3:
    m3.update(d.encode('utf8'))


# Create an LSH Ensemble index with threshold and number of partition
# settings.
lshensemble = MinHashLSHEnsemble(threshold=0.8, num_perm=128,
    num_part=32)
logging.debug(lshensemble)

# Index takes an iterable of (key, minhash, size)
lshensemble.index([("m2", m2, len(set2)), ("m3", m3, len(set3))])
#print(lshensemble.indexes[0])

# Check for membership using the key
print("m2" in lshensemble)
print("m3" in lshensemble)

# Using m1 as the query, get an result iterator
print("Sets with containment > 0.8:")
print(len(set1))
print(len(set2))
print(len(set3))
for key in lshensemble.query(m1, len(set1)):
    #logging.debug(lshensemble.query(m1, len(set1)))
    #print(lshensemble.query(m1,len(set1)))
    print(key)
