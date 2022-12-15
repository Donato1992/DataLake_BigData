import sys
import os
import pandas as pd
import math
import json 

DEFAULT_DIR = "datasets/"
DEFAULT_COD_DIR = "_cod_lists/"
DEFAULT_ENTRY_FILE = DEFAULT_COD_DIR+"list_sources"

def import_file(dir_path,file_path,suffix):
	filename = dir_path+file_path+"."+suffix
	print("Importing data from "+filename+" ...")
	#read the file
	df = pd.read_csv(dir_path+file_path+"."+suffix, dtype='unicode')
	#con list_df prendo le intestazioni dei nostri file csv [dataset]
	list_df = list(df)
	num_rows = df.shape[0]
	num_columns = len(list_df)
	print("\tAnalysing "+str(num_rows)+" rows\n\tShrinking data")
	# iterate columns
	for i in list_df:
		column_values = df[i].tolist()
		#print("Lunghezza Inizio-->"+str(i)+str(len(column_values)))
		#deduplicate values ==> reduces the size of indexing structures by 90%
		#column_values = set(column_values)
		#print("Lunghezza deduplicata-->"+str(i)+str(len(column_values)))
		column_values = list(filter(None, column_values)) # remove null values
		#print("Numero di Valori per la Colonna--->"+str(len(column_values)))
		#store in file
		with open(DEFAULT_COD_DIR+file_path+"."+str(list_df.index(i)),"w", encoding="utf-8") as f:
			#print("VEDIAMO IL FILE CHE SCRIVO"+file_path)
			#print(column_values)
			#print(DEFAULT_COD_DIR+file_path+"."+str(list_df.index(i)))
			for s in column_values:
				if isinstance(s,str) or not math.isnan(s):
					f.write(str(s)+"\n")
	entry = {"num_rows":num_rows, "num_columns":num_columns}
	update_entry(filename,entry)
	print("\tStored: "+str(num_columns)+" columns")


def update_entry(filename,entry):
	with open(DEFAULT_ENTRY_FILE,"r") as f:
		data_decoded = json.load(f)
	#update or save
	data_decoded[filename]=entry
	with open(DEFAULT_ENTRY_FILE,"w", encoding="utf-8") as f:
		json.dump(data_decoded,f, indent=4, sort_keys=True)


def main():
    # Import a single dataset
	if len(sys.argv)>1:
		if os.path.exists(sys.argv[1]):
			path = sys.argv[1]
			mydir=path[:path.rfind("/")+1]
			filename=path[path.rfind("/")+1:path.rfind(".")]
			suffix=path[path.rfind(".")+1:]
			import_file(mydir,filename,suffix)
		else:
			print("Error: no such file.")
			
	# Import all datasets from the DEFAULT_DIR		
	else:
		all_files = [f for f in os.listdir(DEFAULT_DIR) if os.path.isfile(os.path.join(DEFAULT_DIR, f))]
		for myfile in all_files:
			myfile = myfile[:myfile.rfind(".")]
			import_file(DEFAULT_DIR,myfile,"csv")

if __name__ == "__main__":
    main()

