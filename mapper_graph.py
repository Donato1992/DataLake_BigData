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
import rdflib
import re
import asyncio
import owlrl

#Debugging
LOG_FILE='test_graph_nofile_dict.log'
if os.path.exists(LOG_FILE):
	os.remove(LOG_FILE)
logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG)




DEFAULT_DIR = "datasets/"
DEFAULT_DIMENSIONS_DIR="dimensions/"
DEFAULT_COD_DIR = "_cod_lists/"
DEFAULT_DIM_CHECK="filter.csv"
GEO_CONTINENT="./dimensions/Geo.continent"
DEFAULT_ENTRY_FILE = DEFAULT_COD_DIR+"list_sources"
COLUMS_JOINABLE="./joinable.json"
NUM_PERM = 512
NUM_PART = 32
THRESHOLD = 0.8
DICTIONARY_CONTINENT={}
DICTIONARY_FREQUENCY={}
DATA=[]
LABEL=[]
CONTINET_PERCENT=pd.DataFrame([],
	[],
	columns=['Percent']
	)
GRAPH = rdflib.Graph()
GRAPH.parse("knowladge_graph.ttl",format="turtle")

def map_file(mydir, filename, suffix):
	print("Initializing the mapper...")
	# Create an LSH Ensemble index with threshold and number of partition settings.
	lshensemble = MinHashLSHEnsemble(threshold=THRESHOLD, num_perm=NUM_PERM, num_part=NUM_PART)

	# Initialize LSHEnsemble
	
	#Retrieve dimensions from the graph
	q_result=GRAPH.query("""SELECT ?x ?y  WHERE {?x ?z ?y.FILTER(?z=property:inLevel && ?x!=member:month && ?x!=member:year)}""")
	my_dimension=[]
	index=[]
	for row in q_result:
		my_dimension.append(str(row.y).split("#")[1])

	my_dimension=list(set(my_dimension))

	for x in my_dimension:
		m = MinHash(num_perm=NUM_PERM)
		counter=0
		for row in q_result:
			# Create the MinHash for the i-th level
			# encoding = 'ISO-8859-1' in caso estremo ma meglio utilizzare utf-8 in quanto ha molto più caratteri
			dimension=str(row.y).split("#")[1]
			content=str(row.x).split("#")[1]
			# Update the MinHash
			if x==dimension:
				counter=counter+1
				m.update_batch([content.encode('utf8')])
		index.append(tuple((x,m,counter)))
		
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

		#Here we extend the graph in such a way as to obtain logical inferences (Consider creating two files for the two graphs)
		owlrl.DeductiveClosure(owlrl.CombinedClosure.RDFS_OWLRL_Semantics,datatype_axioms=True).expand(GRAPH)
		q_result=GRAPH.query("""SELECT ?x ?y WHERE{{SELECT DISTINCT ?x ?y WHERE{?x property:rollup ?y. ?x property:inLevel level:country} ORDER BY ASC(?X)} UNION {SELECT DISTINCT ?x ?y WHERE{?x property:rollup ?k. ?k property:rollup ?y.?x property:inLevel level:region}ORDER BY ASC(?X)}}""")
		#In this line I fill the dictionary
		for row in q_result:
			DICTIONARY_CONTINENT[str(row.x).split("#")[1]]=str(row.y).split("#")[1]

		
		for c in range(entry["num_columns"]):
			
			
			m1 = MinHash(NUM_PERM)
			with open(DEFAULT_COD_DIR+filename+"."+str(c),"r") as col:
				startTimeHashing = time.time()
				values=col.read().split("\n")
				valori=set(values)
				for v in valori:
					m1.update(v.encode('utf8'))
				#m1.update_batch([s.encode('utf8') for s in values])
				durationHashing = time.time() - startTimeHashing
				durationsHashing.append(durationHashing)
				startTimeQuery = time.time()
				for mapping in lshensemble.query(m1, len(valori)):		
					print("Column "+str(c)+" -> "+mapping)
					colums_joinable(mapping,filename,c)
					logging.debug("Column "+str(c)+" -> "+mapping)
					asyncio.run(frequency(values,mapping))
				with open(filename+'_json_data.json', 'w', encoding="utf-8") as outfile:
					json.dump(DICTIONARY_FREQUENCY, outfile, 
                        indent=4,  
                        separators=(',',': '))
			
				durationQuery = time.time() - startTimeQuery
				durationsQuery.append(durationQuery)
		sum_durations = sum(durationsHashing)
		print("Sum durations hashing = "+str(sum_durations))
		print("Avg durations hashing = "+str((sum_durations/len(durationsHashing))))
		sum_durations_query = sum(durationsQuery)
		print("Sum durations query = "+str(sum_durations_query))
		print("Avg durations query = "+str((sum_durations_query/len(durationsQuery))))
		logging.debug(DICTIONARY_FREQUENCY)

		
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
			if os.path.exists(COLUMS_JOINABLE):
				map_file(mydir,filename,suffix)
			else:
				with open(COLUMS_JOINABLE, 'w') as f_json:
					print("The json file is created")
				f_json.close()
				map_file(mydir,filename,suffix)
		else:
			print("Error: no such file.")
	else:
		print("Error: invalid call. Usage: mapper [list | source sourcename]")

async def frequency(values,type_dimension):
	freq = {}
	dizionario_key={}
	# We calculate the percentage for the single item
	dataframe_continent=continent_analysis(CONTINET_PERCENT)
	try:
		date_str = str(item)
		if(date_str!=""):
				now = datetime.strptime(date_str, '%Y-%m-%d').date()
				#year = now.strftime("%Y")
				#year=int(year)
				if (now in freq):
					freq[now] += 1
				else:
					freq[now] = 1
		
		for key, value in freq.items():
			temp=[]
			dimension_colum=len(values)
			percentual_time=str(round(value/dimension_colum*100, 2))+"%"
			print ("% d : % d : %s"%(key, value, percentual_time))
			temp.append([value,str(str(percentual_time))])
			dizionario_key[key]=dict(temp)
		DICTIONARY_FREQUENCY[type_dimension]=dict(dizionario_key)
	except Exception as e:
		print ("Format Date not valid")
	
	for item in values:
		if(item!=""):
			if (item in freq):
				freq[item] += 1

			else:
				freq[item] = 1

	numeber_sum=0

	
	for key, value in freq.items():
		temp=[]
		dimension_colum=len(values)
		number_percent=round(float(value)/dimension_colum*100, 2)
		numeber_sum=numeber_sum+number_percent
		percentual_value=str(number_percent)+"%"

		task=asyncio.create_task(all_continent(key,dataframe_continent,number_percent))
		await task
		
		print ("% s : % d : %s"%(key, value,percentual_value))
		temp.append([value,str(str(percentual_value))])
		dizionario_key[re.sub('[^0-9a-zA-Z-]', '_', key)]=dict(temp)
	logging.debug("MIA SOMMA-->"+str(numeber_sum))
	DICTIONARY_FREQUENCY[type_dimension]=dict(dizionario_key)	

	# CONTROLLARE SE IL DATAFRAME E' VUOTO NON LO INSERIRE NEL LOG
	logging.debug(dataframe_continent)
	print(dataframe_continent)
	DICTIONARY_FREQUENCY["rollup_"+str(type_dimension)]=dict(dataframe_continent.to_dict())

def continent_analysis(dataframe):
	read_continent = open(GEO_CONTINENT, "r")
	rows=[]
	for x in read_continent:
		rows.append(str(x.splitlines()[0]))
	length=range(len(rows))
	dataframe_new=pd.DataFrame([None for x in length],
	rows,
	['Percent'])
	return dataframe_new


async def all_continent(item,dataframe,percent_value):
	
	item=re.sub('[^0-9a-zA-Z-]', '_', item)
	
	#check whether the element is present in the graph using the dictionary
	#later if it exists I rollup for the continent
	try:
		if DICTIONARY_CONTINENT[item]:
			if dataframe.loc[DICTIONARY_CONTINENT[item],"Percent"]==None:
					percent_sum=round(0.00+percent_value,2)
					dataframe.loc[DICTIONARY_CONTINENT[item],"Percent"]=percent_sum
			else:
				percent_sum=round(float(dataframe.loc[DICTIONARY_CONTINENT[item],"Percent"])+percent_value,2)
				dataframe.loc[DICTIONARY_CONTINENT[item],"Percent"]=percent_sum

	except KeyError:
		
		print("Not Exsits-->"+str(item)+"in Graph")
		logging.debug("Non Esiste-->"+str(item))
	
		#Print used only to see the results of the query	
		#print(f" {row.x}")		


def colums_joinable(dimension,dataset,colum):
	dictObj = []
 
	# Check if file exists
	if os.path.isfile(COLUMS_JOINABLE) is False:
		raise Exception("File not found")
	
	# Read JSON file
	with open(COLUMS_JOINABLE) as fp:
		try:
			dictObj = json.load(fp)
		except:
			dictObj=dict()
		print("File Json Empty")

	if bool(dictObj):
		try:
			if dictObj[dimension]:
				dictObj[dimension].update({dataset: colum})
		except:
			dictObj[dimension]=({dataset: colum})

	else:
		dictObj[dimension]=({dataset: colum})

	
	# Verify updated dict
	print(dictObj)
	
	with open(COLUMS_JOINABLE, 'w', encoding="utf-8") as json_file:
		json.dump(dictObj, json_file, 
							indent=4,  
							separators=(',',': '))
	
	print('Successfully written to the JSON file')
	
	
if __name__ == "__main__":
    main()
