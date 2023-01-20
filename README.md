# Progetto di Big Data 2022

Progetto sui Data Lake

Il progetto è stato suddiviso in cartelle e file su cui consigliamo di mantenere le stesse directory dei file. 
* Nella cartella datasets sono prenti i file dei datasets in formato csv e un file json contenente la lista dei datasets oggetti di analisi dove andiamo a riportare anche il numero di righe e di colonne dei rispettivi datasets. Tuttavia non è presente nel github in quanto la dimensione di essa è molto elevata e di conseguenza non è stato possibile caricarlo. Per ovviare a tale problema, i datasets sono rimasti salvati in locale ed è stato generato il file .gitignore dove abbiamo elencato tutti i file da non prendere in considerazione durante la fase di push.
* La cartella datasets_json_map è composta da file json generati durante l'esecuzione del mapper_graph.py. In tali file, vengono inseriti i metadati di ciascun dataset mappato.
* La cartella dimensions è costituita da cartelle e file utili per la creazione del knowledge-graph.ttl [In linea teorica tale cartella potrebbe essere eliminata dopo la creazione del knowledge-graph.ttl. Tuttavia consigliamo di non eliminarla in quanto il danneggiamento o la perdita del knowledge-graph.ttl porterebbe ad una totale assenza di informazioni in assenza di tali file.]
* Il file check_joinability_new.py serve per verificare la joinabilità tra due datasets sorgenti.
* Il file create_graph.py permette di poter creare il knowledge-graph.ttl
* Il file joinable.json viene generato automaticamente durante l'esecuzione del mapper.graph.py. Esso contiente per ogni dimensione i datasets corrispondenti mappati.
* Il file knowledge-graph.ttl è la serializzazione in turtle del knowledge-graph.
* Il file mapper_graph.py mappa i datasets con le corrispettive dimensioni e livelli del knowledge-graph.
* I file con l'estensione .log sono dei file di log generati per poter visualizzare su un file i risultati dell'esecuzione dei corrispettivi codici in python.

# Installazione e configurazine

* Versione python utilizzata: 3.10.8
* Installare la libreria owlrl eseguendo da terminale: pip3 Install owlrl
* Per eseguire il mapper_graph.py bisogna digitare da riga di comando il datasets da analizzare: 
    - python3 mapper_graph.py source datasets/[nome_dataset].csv 
* Per l'esecuzione del check_joinability_new.py bisogna digitare da riga di comando i datasets da analizzare:
    - python3 check_joinability_new.py [nome_dataset_1].csv [nome_dataset_2].csv


* Di Zinno Donato
* Tigano Costantino
