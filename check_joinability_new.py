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
    lista_dataset=[]
    comando=sys.argv
    lista_dataset=comando.split(" ")
    print(lista_dataset)
	


if __name__ == "__main__":
    main()