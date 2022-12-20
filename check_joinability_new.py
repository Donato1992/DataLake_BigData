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

    #Load First Json Dataset File
    with open(check_dataset[0]+"_json_data.json","r",encoding="utf-8") as ffile:
        dict_first=json.load(ffile) 
    #Load Second Json Dataset file
    with open(check_dataset[1]+"_json_data.json","r",encoding="utf-8") as sfile:
        dict_second=json.load(sfile) 
    
    for type_dimension in data_set.keys():
        print(type_dimension)
        try:
            # Utilizzo l'or per far si che non va in errore qualora il "valore" che vede durante l'if nel caso in cui
            # è uno zero non va in false
            if (data_set[type_dimension][check_dataset[0]] or data_set[type_dimension][check_dataset[0]]==0) and (data_set[type_dimension][check_dataset[1]] or data_set[type_dimension][check_dataset[1]]==0):
                print("Same dimension: "+str(type_dimension))
                n_first_keys=len(list(dict_first[type_dimension].keys()))
                n_second_keys=len(list(dict_second[type_dimension].keys()))
            
                if n_first_keys>=n_second_keys:
                    
                    for x_key in list(dict_first[type_dimension].keys()):
                        dataset_value_1=check_argument(dict_first,type_dimension,x_key,check_dataset[0])
                        dataset_value_2=check_argument(dict_second,type_dimension,x_key,check_dataset[1])

                        # Nella rollup lo fa n volte perchè nel caso di progetti futuri si poteva fare anche un analisi per singolo
                        # "argomento"
                        if isinstance(dataset_value_1,dict) and isinstance(dataset_value_2,dict):
                            lenght_rollup=range(len(list(dataset_value_1.values())))
                            for n in lenght_rollup:
                                if list(dataset_value_1.values())[n]==None or list(dataset_value_2.values())[n]==None:
                                    print("Not confrontable")
                                else:
                                    if abs(float(list(dataset_value_1.values())[n])-float(list(dataset_value_2.values())[n]))<2.00:
                                        print("Joinable in "+str(list(dataset_value_1.keys())[n]))
                                    else:
                                        print("Not Joinable in "+str(list(dataset_value_1.keys())[n]))
                        
                            
                        else:
                            if(float(dataset_value_1.split("%")[0])>30.00 and float(dataset_value_2.split("%")[0])>30.00):
                                print("Datasets are joinable in "+str(x_key))
                            elif abs(float(dataset_value_1.split("%")[0])-float(dataset_value_2.split("%")[0]))>5.00:
                                print("Datasets non joinable in "+str(x_key))
                            else:
                                print("joinable in "+str(x_key))

                         
                else:
                    for x_key in n_second_keys:
                        check_argument(dict_first,type_dimension,x_key)
                        check_argument(dict_first,type_dimension,x_key,check_dataset[0])
                

                    
                dimension_joinable.append(type_dimension)
        except Exception as e:
            print("Not present "+str(e.args)+" in dimension joinable "+str(type_dimension))

def check_argument(dict_check,dimensions,keys,datasets):
    try:
        if dimensions != "day":
            return dict_check["rollup_"+dimensions]["Percent"]
        else:
            
            return str(list(dict_check[dimensions][keys].values())[0])

        #return list(dict_check[dimensions][keys].values())[0]
    except Exception as e:
        print("Not present the argument "+str(e.args[0])+" in dataset "+str(datasets))

<<<<<<< HEAD
=======
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

    
>>>>>>> c3e096e343176d34a3f579558f2686b0aa783be7


if __name__ == "__main__":
    main()