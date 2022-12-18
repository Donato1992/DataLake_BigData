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
    # Map single dataset
    check_dataset=[sys.argv[1].split(".csv")[0], sys.argv[2].split(".csv")[0]]
    with open('joinable.json', 'r', encoding="utf-8") as outfile:
        data_set=json.load(outfile)
    
    for type_dimension in data_set.items():
        try:
            print(str(type_dimension))
            # Utilizzo l'or per far si che non va in errore qualora il "valore" che vede durante l'if nel caso in cui
            # è uno zero non va in false
            if (data_set[type_dimension][check_dataset[0]] or data_set[type_dimension][check_dataset[0]]==0) and (data_set[type_dimension][check_dataset[1]] or data_set[type_dimension][check_dataset[1]]==0):
                print("Parlano la stessa dimensione la stessa dimensione"+str(type_dimension))
        except Exception as e:
            print("Non e' presente "+str(e.args)+" nella dimensione joinable "+str(type_dimension))



	


if __name__ == "__main__":
    main()