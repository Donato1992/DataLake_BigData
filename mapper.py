import sys
import logging
import os
import csv
import pandas as pd
from itertools import zip_longest
import math
import json
from datasketch import MinHashLSHEnsemble, MinHash
import time
from datetime import datetime

#Attivo il Debugging
os.remove("test.log")
logging.basicConfig(filename='test.log',level=logging.DEBUG)




DEFAULT_DIR = "datasets/"
DEFAULT_DIMENSIONS_DIR="dimensions/"
DEFAULT_COD_DIR_T="dimensions/filter/"
DEFAULT_COD_DIR = "_cod_lists/"
DEFAULT_DIM_CHECK="filter.csv"
GEO_CONTINENT="./dimensions/Geo_Continent/Geo.continent"
DEFAULT_ENTRY_FILE = DEFAULT_COD_DIR+"list_sources"
print(DEFAULT_ENTRY_FILE)
NUM_PERM = 512
NUM_PART = 32
THRESHOLD = 0.8
DATA=[]
LABEL=[]
DIMENSIONS=["Time.day","Geo.region_iso","Geo.region","Geo.country_iso2","Geo.country","Geo.country_iso3","Geo.continent"]
CONTINET_PERCENT=pd.DataFrame([],
	columns=['Country', 'Percent'])

def map_file(mydir, filename, suffix):
	print("Initializing the mapper...")
	# Create an LSH Ensemble index with threshold and number of partition settings.
	lshensemble = MinHashLSHEnsemble(threshold=THRESHOLD, num_perm=NUM_PERM, num_part=NUM_PART)

	# Initialize LSHEnsemble
	# RICORDATI DI SPOSTARE EVENTUALMENTE LE MIE DIMENSIONI GEO.CONTINENT IN UN ALTRA CARTELLA
	index = []
	all_dimension_files = [f for f in os.listdir(DEFAULT_DIMENSIONS_DIR) if os.path.isfile(os.path.join(DEFAULT_DIMENSIONS_DIR, f))]
	for f in all_dimension_files:
		# Create the MinHash for the i-th level
		# encoding = 'ISO-8859-1' in caso estremo ma meglio utilizzare utf-8 in quanto ha molto più caratteri
		m = MinHash(num_perm=NUM_PERM)
		with open(DEFAULT_DIMENSIONS_DIR+f, "r") as dimFile:
			#La riga qui sotto mi serve per vedere se mi crea file _DS STORE
			#logging.debug(dimFile)
			content=dimFile.read().split("\n")
			# Update the MinHash
			m.update_batch([s.encode('utf8') for s in content])
			#for d in content:
			#	m.update(d.encode('utf8'))	
			index.append(tuple((f,m,len(content))))
			#print(index)
		
	lshensemble.index(index)		

	

	# Read the entry for the input file
	print("Reading source metadata...")
	with open(DEFAULT_ENTRY_FILE,"r") as entryF:
		#print(DEFAULT_ENTRY_FILE)
		entries_decoded=json.load(entryF)
		entry = entries_decoded[mydir+filename+"."+suffix]
		#for each column of the data source
		durationsHashing=[]
		durationsQuery=[]
		for c in range(entry["num_columns"]):
			#print("QUIIIII")
			#print(entry["num_columns"])
			m1 = MinHash(NUM_PERM)
			with open(DEFAULT_COD_DIR+filename+"."+str(c),"r") as col:
				#print(DEFAULT_COD_DIR+filename+"."+str(c))
				startTimeHashing = time.time()
				values=col.read().split("\n")
				valori=set(values)
				print("Vediamo"+str(len(valori)))
				for v in valori:
					m1.update(v.encode('utf8'))
				#m1.update_batch([s.encode('utf8') for s in values])
				durationHashing = time.time() - startTimeHashing
				durationsHashing.append(durationHashing)
				startTimeQuery = time.time()
				for mapping in lshensemble.query(m1, len(valori)):		
					print("Column "+str(c)+" -> "+mapping)
					logging.debug("Column "+str(c)+" -> "+mapping)
					colums_joinable(mapping,values,filename)
					frequency(values,mapping)
				durationQuery = time.time() - startTimeQuery
				durationsQuery.append(durationQuery)
		sum_durations = sum(durationsHashing)
		print("Sum durations hashing = "+str(sum_durations))
		print("Avg durations hashing = "+str((sum_durations/len(durationsHashing))))
		sum_durations_query = sum(durationsQuery)
		print("Sum durations query = "+str(sum_durations_query))
		print("Avg durations query = "+str((sum_durations_query/len(durationsQuery))))

		
def read_entries():
	with open(DEFAULT_ENTRY_FILE,"r") as f:
		data_decoded = json.load(f)
		print(data_decoded.keys())


def main():
	# Map single dataset
	print(sys.argv[1])
	if (len(sys.argv)==2 and sys.argv[1]=="list"):
		#List all sources
		with open(DEFAULT_ENTRY_FILE,"r") as f:
			data_decoded=json.load(f)
			for k in data_decoded.keys():
				print(k)
	elif (len(sys.argv)==3 and sys.argv[1]=="source"):
		if(os.path.exists(sys.argv[2])):
			path = sys.argv[2]
			mydir=path[:path.rfind("/")+1]
			filename=path[path.rfind("/")+1:path.rfind(".")]
			suffix=path[path.rfind(".")+1:]
			map_file(mydir,filename,suffix)
		else:
			print("Error: no such file.")
	else:
		print("Error: invalid call. Usage: mapper [list | source sourcename]")

def colums_joinable(colum_ok,values,filename):
	#store in file
	
	#Utilizzare a se voglio aggiornare il file
	#list_colums_joinable(str(colum_ok+"."+filename))
	with open(DEFAULT_COD_DIR_T+str(colum_ok+"."+filename),"w",encoding="UTF-8") as f:
		for s in values:
			if isinstance(s,str) or not math.isnan(s):
				f.write(str(s)+"\n")
	f.close()


#Function NOT USE NOW
def list_colums_joinable(list_joinable):
	#store in file for List Joinable colum
	if(os.path.isfile(DEFAULT_COD_DIR_T+DEFAULT_DIM_CHECK)):
		with open(DEFAULT_COD_DIR_T+DEFAULT_DIM_CHECK,"a",encoding="UTF-8") as f:
			f.write(str(list_joinable)+"\n")
		f.close()
	else:
		the_list=[list_joinable]
		data={
			'Joinable':the_list
			}
		df=pd.DataFrame(data)
		df.to_csv(DEFAULT_COD_DIR_T+DEFAULT_DIM_CHECK, index= False)

def frequency(values,type_dimension):
	freq = {}
	dataframe_continent=continent_analysis(CONTINET_PERCENT)
	if (type_dimension=="Time.day"):
		for item in values:
			date_str = str(item)
			if(date_str!=""):
				now = datetime.strptime(date_str, '%Y-%m-%d').date()
				year = now.strftime("%Y")
				year=int(year)
				if (year in freq):
					freq[year] += 1
				else:
					freq[year] = 1
		
		for key, value in freq.items():
			dimension_colum=len(values)
			percentual_time=str(round(value/dimension_colum*100, 2))+"%"
			print ("% d : % d : %s"%(key, value, percentual_time))
	else:
		for item in values:
			if(item!=""):
				if (item in freq):
					freq[item] += 1

				else:
					freq[item] = 1
		
		numeber_somma=0
		cont=0
		for key, value in freq.items():
			dimension_colum=len(values)
			number_percent=round(value/dimension_colum*100, 2)
			numeber_somma=numeber_somma+number_percent
			percentual_value=str(number_percent)+"%"
			cont=cont+1
			all_continent(key,dataframe_continent,number_percent)
			print ("% s : % d : %s"%(key, value,percentual_value))
			#Inizio per il LOGGING
			logging.debug(str(key)+":"+str(value)+":"+str(percentual_value))
		logging.debug("SOMMA PERCENTUALE:-->"+str(numeber_somma))
		logging.debug("Country passati-->"+str(cont))
		logging.debug(dataframe_continent)
		#Fine per il LOGGING
		print(dataframe_continent)

def continent_analysis(dataframe):
	read_continent = open(GEO_CONTINENT, "r")
	dataframe_new=pd.DataFrame([],
	columns=['Country', 'Percent'])
	for x in read_continent:
		dataframe_new.loc[len(dataframe_new),['Country','Percent']]=[x.splitlines()[0],[]]
	frames=[dataframe,dataframe_new]
	result=pd.concat(frames,ignore_index=True)
	return result


def all_continent(country,dataframe,percent_value):
	read_continent = open(GEO_CONTINENT, "r")
	continet=[]
	for x in read_continent:
		continet.append(x.splitlines()[0])
	index=0
	cc=0
	for c in continet:
		if os.path.exists(GEO_CONTINENT+"."+c):
			if dataframe.loc[index,"Percent"]==[]:
				percent_sum=0+percent_value
			else:
				#print(dataframe.loc[index,"Percent"])
				percent_sum=int(dataframe.loc[index,"Percent"])+percent_value

			with open(GEO_CONTINENT+"."+c, encoding = 'utf-8') as read_country:
				for line in read_country:
					if line.strip()==country:
						dataframe.loc[index,"Percent"]=percent_sum
						#Inizio per il LOGGING

						#logging.debug("Valore 1:"+str(line.strip())+"-"+"Valore 2:"+str(country)+":"+"MATCHATO---->SI")
						cc=cc+1
						#logging.debug("KKTRACKING-->"+str(cc))

						#Fine per il LOGGING
					else:	
						#Inizio per il LOGGING
						flag=0
						#logging.debug("Valore 1:"+str(line.strip())+"-"+"Valore 2:"+str(country)+":"+"MATCHATO---->NO")
						#Fine per il LOGGING
				index=index+1		
		else:
			#Other Valori Inesistenti
			index=index+1
		
	
if __name__ == "__main__":
    main()
