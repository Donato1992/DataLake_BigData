import sys
import os
import pandas as pd
import math
import json
from datasketch import MinHashLSHEnsemble, MinHash
import time
import logging

#Debug File
LOG_FILE='./check_joinability.log'

if os.path.exists(LOG_FILE):
	os.remove(LOG_FILE)
else:
    with open(LOG_FILE, 'w') as f_json:
        print("The json file is created")
    f_json.close()
logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG)

DEFAULT_DIR = "datasets/"
DEFAULT_DIMENSIONS_DIR="dimensions/"
JSON_FOLDER="datasets_json_map/"
NUM_PERM = 256
NUM_PART = 32
THRESHOLD = 0.95
THRESHOLD_VALUE=5.00


def main():

    
    dimension_joinable=[]
    # Map single dataset
    check_dataset=[sys.argv[1].split(".csv")[0], sys.argv[2].split(".csv")[0]]
    #check_dataset=["bing_covid-19_data", "covid-hospitalizations"]

    with open('joinable.json', 'r', encoding="utf-8") as outfile:
        data_set=json.load(outfile)

    #Load First Json Dataset File
    with open(JSON_FOLDER+check_dataset[0]+"_json_data.json","r",encoding="utf-8") as ffile:
        dict_first=json.load(ffile) 
    #Load Second Json Dataset file
    with open(JSON_FOLDER+check_dataset[1]+"_json_data.json","r",encoding="utf-8") as sfile:
        dict_second=json.load(sfile)

    c1=[]
    c2=[] 
    
    for type_dimension in data_set.keys():
        try:
            # Utilizzo l'or per far si che non va in errore qualora il "valore" che vede durante l'if nel caso in cui
            # è uno zero non va in false
            if (data_set[type_dimension][check_dataset[0]] or data_set[type_dimension][check_dataset[0]]==0) and (data_set[type_dimension][check_dataset[1]] or data_set[type_dimension][check_dataset[1]]==0):
                print("Same dimension: "+str(type_dimension))
                if len(c1)==0 and len(c2)==0:
                    c1.append(data_set[type_dimension][check_dataset[0]])
                    c2.append(data_set[type_dimension][check_dataset[1]])
                else:
                    c1.insert(1,data_set[type_dimension][check_dataset[0]])
                    c2.insert(1,data_set[type_dimension][check_dataset[1]])
                    #check_joinability("bing_covid-19_data.csv", "covid-hospitalizations.csv", c1,c2)
                    check_joinability(sys.argv[1], sys.argv[2], c1,c2)
                n_first_keys=len(list(dict_first[type_dimension].keys()))
                n_second_keys=len(list(dict_second[type_dimension].keys()))
            
                if n_first_keys>=n_second_keys:
                    
                    for x_key in list(dict_first[type_dimension].keys()):
                        flag_rollup=False
                        dataset_value_1=check_argument(dict_first,type_dimension,x_key,check_dataset[0],flag_rollup)
                        dataset_value_2=check_argument(dict_second,type_dimension,x_key,check_dataset[1],flag_rollup)


                        # Nella rollup lo fa n volte perchè nel caso di progetti futuri si poteva fare anche un analisi per singolo
                        # "argomento"
                        try:
                            if abs(float(dataset_value_1.split("%")[0])-float(dataset_value_2.split("%")[0]))>THRESHOLD_VALUE:
                                print("Datasets non joinable in "+str(x_key))
                            else:
                                #print("joinable in "+str(x_key))
                                logging.debug("joinable in "+str(x_key))
                        except Exception as e:
                            e
                            #print("Errore"+str(e.args))
                    flag_rollup=True
                    dataset_value_1=check_argument(dict_first,type_dimension,x_key,check_dataset[0],flag_rollup)
                    dataset_value_2=check_argument(dict_second,type_dimension,x_key,check_dataset[1],flag_rollup)
                    check_rollup(dataset_value_1,dataset_value_2,type_dimension)
                            
                else:
                    
                    for x_key in list(dict_second[type_dimension].keys()):
                        flag_rollup=False
                        dataset_value_1=check_argument(dict_first,type_dimension,x_key,check_dataset[0],flag_rollup)
                        dataset_value_2=check_argument(dict_second,type_dimension,x_key,check_dataset[1],flag_rollup)

                        try:
                            if abs(float(dataset_value_1.split("%")[0])-float(dataset_value_2.split("%")[0]))>THRESHOLD_VALUE:
                                print("Datasets non joinable in "+str(x_key))
                            else:
                                #print("joinable in "+str(x_key))
                                logging.debug("joinable in "+str(x_key))
                        except Exception as e:
                            e
                            #print("Errore"+str(e.args))
                    flag_rollup=True
                    dataset_value_1=check_argument(dict_first,type_dimension,x_key,check_dataset[0],flag_rollup)
                    dataset_value_2=check_argument(dict_second,type_dimension,x_key,check_dataset[1],flag_rollup)
                    check_rollup(dataset_value_1,dataset_value_2,type_dimension) 

                    
                dimension_joinable.append(type_dimension)
        except Exception as e:
            e
            #If you want to see which data is in one dataset and not the other, uncomment the line below
            #print("Not present "+str(e.args)+" in dimension joinable "+str(type_dimension))

def check_argument(dict_check,dimensions,keys,datasets,rollup):
    try:
        
        if rollup:
            
            return dict_check["rollup_"+dimensions]["Percent"]
        else:
            
            return str(list(dict_check[dimensions][keys].values())[0])

        #return list(dict_check[dimensions][keys].values())[0]
    except Exception as e:
        e
        #print("Not present the argument "+str(e.args[0])+" in dataset "+str(datasets))

def check_rollup(dataset_value_1,dataset_value_2,dimension):
    lenght_rollup=range(len(list(dataset_value_1.values())))
    for n in lenght_rollup:
        if list(dataset_value_1.values())[n]==None or list(dataset_value_2.values())[n]==None:
            print("Not confrontable")
        else:
            if abs(float(list(dataset_value_1.values())[n])-float(list(dataset_value_2.values())[n]))<2.00:
                #print("Joinable in "+str(list(dataset_value_1.keys())[n]))
                logging.debug("Joinable in "+"rollup "+str(dimension)+":"+str(list(dataset_value_1.keys())[n]))
            else:
                print("Not Joinable in "+str(list(dataset_value_1.keys())[n]))


def check_joinability(dataset_1,dataset_2, c1,c2):
    s1="datasets/"+str(dataset_1)
    s2="datasets/"+str(dataset_2)

    m1 = MinHash(num_perm=NUM_PERM)
    m2 = MinHash(num_perm=NUM_PERM)

    
    df1 = pd.read_csv(s1, dtype='unicode',usecols=c1)
    list_df1 = list(df1)
    df2 = pd.read_csv(s2, dtype='unicode',usecols=c2)
    list_df2 = list(df2)

    
    if (c1==sorted(c1)):
        df1_total=df1[list_df1[0]].astype(str) + " " + df1[list_df1[1]].astype(str)
    else:
        df1_total=df1[list_df1[1]].astype(str) + " " + df1[list_df1[0]].astype(str)
    
    if (c2==sorted(c2)):
        df2_total=df2[list_df2[0]].astype(str) + " " + df2[list_df2[1]].astype(str)
    else:
        df2_total=df2[list_df2[1]].astype(str) + " " + df2[list_df2[0]].astype(str)
    start = time.time()
	
	#update the combined MinHashes
    print("\tcreating hashes")
    startm1 = time.time()
    for i in df1_total:
        m1.update(i.encode('utf8'))
    print("Time m1: "+str(time.time()-startm1)+"\n")
	
    startm2 = time.time()
    for i in df2_total:
        m2.update(i.encode('utf8'))	
    print("Time m3: "+str(time.time()-startm2)+"\n")
	

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
        #print("First = "+str(first)+" Last: "+str(last)+"\n")
		# Check if source 1 is joinable to source 5. Then it needs to be done the other way round, between s5 and s1. The highest of the two final values is kept.
        for k in lshensemble.query(m2,len(df2_total)):
            #print(k)
            #print("Result:\n\tsources joinable with containment > "+str(q))
            found = True
        if(found):
            first = q
            sure = q
        else:
            last = q	
	
    print("Result:\n\tsources joinable with containment > "+str(sure))
    logging.debug("Result:\n\tsources joinable with containment > "+str(sure))
		
    final_time = time.time()
    hashing_time = end_hashing - start
    querying_time = final_time - end_hashing
    print("Hashing time:"+str(hashing_time))	
    print("Querying time:"+str(querying_time))	
    print("Total time:"+str(hashing_time + querying_time))





if __name__ == "__main__":
    main()