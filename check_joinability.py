import sys
import os
import pandas as pd
import math
import json
from datasketch import MinHashLSHEnsemble, MinHash
import time
import logging



if os.path.exists("check_joinability.log"):
	os.remove("check_joinability.log")
logging.basicConfig(filename='check_joinability.log',level=logging.DEBUG)

DEFAULT_DIR = "datasets/"
DEFAULT_DIMENSIONS_DIR="dimensions/"
DEFAULT_COD_DIR = "_cod_lists/"
DEFAULT_ENTRY_FILE = DEFAULT_COD_DIR+"list_sources"
NUM_PERM = 256
NUM_PART = 32
THRESHOLD = 0.95

def main():
	# initialize. This is still hard-coded.
	s1 = "datasets/bing_covid-19_data.csv"
	s3 = "datasets/ecdc_cases.csv"
	s5 = "datasets/covid_policy_tracker.csv"

	#Qua sotto mi prendo le colonne che hanno la data day tempo e le iso_3. Naturalmente le colonne le conosco di default	
	c1 = [11,10]
	c3 = [0,8]
	c5 = [1,36]

	m1 = MinHash(num_perm=NUM_PERM)
	m3 = MinHash(num_perm=NUM_PERM)
	m5 = MinHash(num_perm=NUM_PERM)
	
	print("\treading files")
	#Setting to see all rows of the dataframe
	pd.set_option('display.max_rows', None)	

	df1 = pd.read_csv(s1, dtype='unicode',usecols=c1)
	list_df1 = list(df1)
	df3 = pd.read_csv(s3, dtype='unicode',usecols=c3)
	list_df3 = list(df3)
	df5 = pd.read_csv(s5, dtype='unicode',usecols=c5)
	list_df5 = list(df5)

	#Initialize the combined MinHashes. This needs to be done at setup time, not at query time.
	df1_total=df1[list_df1[0]].astype(str) + " " + df1[list_df1[1]].astype(str)
	df3_total=df3[list_df3[0]].astype(str) + " " + df3[list_df3[1]].astype(str)
	df5_total=df5[list_df5[1]].astype(str) + " " + df5[list_df5[0]].astype(str)

	print("SONO DF1")
	logging.debug("DF1")
	logging.debug(df1_total)
	logging.debug("DF3")
	logging.debug(df3_total)
	logging.debug("DF5")
	logging.debug(df5_total)
	
	print(df1_total)
	
	start = time.time()
	
	#update the combined MinHashes
	print("\tcreating hashes")
	startm1 = time.time()
	for i in df1_total:
			m1.update(i.encode('utf8'))
	print("Time m1: "+str(time.time()-startm1)+"\n")
	
	startm3 = time.time()
	for i in df3_total:
			m3.update(i.encode('utf8'))	
	print("Time m3: "+str(time.time()-startm3)+"\n")
	
	
	startm5 = time.time()
	for i in df5_total:
			m5.update(i.encode('utf8'))				
	print("Time m5: "+str(time.time()-startm5)+"\n")

	end_hashing = time.time()
	
	print("\tchecking joinability")
	#Here we check s5 with s1
	stop = False
	first = 0.0
	last = 1.0
	sure = 0
	#The termination criteria for the dychotomic search is that we got a delta <0.05
	while first<=(last-0.049):
		found = False
		q = (first+last)/2
		lshensemble = MinHashLSHEnsemble(threshold=q, num_perm=NUM_PERM, num_part=NUM_PART)
		lshensemble.index([("s1",m1,len(df1_total))])
		#lshensemble.index([("s5",m5,len(df5_total))])
		print("First = "+str(first)+" Last: "+str(last)+"\n")
		# Check if source 1 is joinable to source 5. Then it needs to be done the other way round, between s5 and s1. The highest of the two final values is kept.
		for k in lshensemble.query(m5,len(df5_total)):
			print(k)
			print("Result:\n\tsources joinable with containment > "+str(q))
			found = True
		if(found):
			first = q
			sure = q
		else:
			last = q	
	
	print("Result:\n\tsources joinable with containment > "+str(sure))
		
	final_time = time.time()
	hashing_time = end_hashing - start
	querying_time = final_time - end_hashing
	print("Hashing time:"+str(hashing_time))	
	print("Querying time:"+str(querying_time))	
	print("Total time:"+str(hashing_time + querying_time))	

	
	
			
# Speciale variabile per poter far eseguire lo script
if __name__ == "__main__":
    main()
