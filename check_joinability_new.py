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


def main():

    dimension_joinable=[]
    # Map single dataset
    check_dataset=[sys.argv[1].split(".csv")[0], sys.argv[2].split(".csv")[0]]
    with open('joinable.json', 'r', encoding="utf-8") as outfile:
        data_set=json.load(outfile)
    
    for type_dimension in data_set.keys():
        print(type_dimension)

        try:
            # Utilizzo l'or per far si che non va in errore qualora il "valore" che vede durante l'if nel caso in cui
            # è uno zero non va in false
            if (data_set[type_dimension][check_dataset[0]] or data_set[type_dimension][check_dataset[0]]==0) and (data_set[type_dimension][check_dataset[1]] or data_set[type_dimension][check_dataset[1]]==0):
                print("Parlano la stessa dimensione: "+str(type_dimension))
                dimension_joinable.append(type_dimension)
        except Exception as e:
            print("Non e' presente "+str(e.args)+" nella dimensione joinable "+str(type_dimension))


    with open(check_dataset[0]+"_json_data.json","r",encoding="utf-8") as ffile:
        dict_first=json.load(ffile)

    with open(check_dataset[1]+"_json_data.json","r",encoding="utf-8") as sfile:
        dict_second=json.load(sfile)


    for item in dimension_joinable:
        if item in dict_first.keys() and item == "day":
            for anno in dict_first[item].keys():
                for tuple in dict_first[item][anno]:
                    dict_first[item][anno][tuple]=dict_first[item][anno][tuple].replace("%%","")
                    if(int(tuple)>100) and (float(dict_first[item][anno][tuple])/100>0.35):
                        print(anno+" "+tuple+" "+dict_first[item][anno][tuple])
                        print("E' joinabile")



                










#può esser fatto meglio, ci vorrebbe un altro filtro per la percentuale, ma devo vedere come convertire in int quella percentuale!
"""
    for x in dict_first.keys():
        for item in dimension_joinable:
            if item==x and x=="day":
                    for anno in dict_first[x].keys():
                        for tuple in dict_first[x][anno]:
                            if(int(tuple)>100):
                                print(anno+" "+tuple+" "+dict_first[x][anno][tuple])
                                print("E' joinabile")

"""

    


if __name__ == "__main__":
    main()