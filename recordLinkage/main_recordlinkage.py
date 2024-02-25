import json
import pandas as pd
from multiprocessing import Manager, Pool, cpu_count, Lock
from record_linkage import RecordLinkageClass

def initialize_lock(l):
    global lock
    lock = l

def initialize_shared_dict(l, d):
    global lock
    global shared_dict
    lock = l
    shared_dict = d

def recordLinkageFromBlocking(key, shared_dict):
    recordLinkageObj = RecordLinkageClass()
    
    with open('recordLinkage/blockingByName.json', 'r') as file:
        data = json.load(file)

    if len(data[key]) > 1:
        dfBlock = pd.DataFrame(data[key])
        dfBlock = dfBlock.drop(columns=['outcome'])
        result = recordLinkageObj.recordLinkageMethod(dfBlock, 'recordLinkage/blockingByName.json', key, shared_dict)
        return result

if __name__ == "__main__":
    with open('recordLinkage/blockingByName.json', 'r') as file:
        data = json.load(file)

    keys = [key for key in data if len(data[key]) > 1]

    # Calcola il numero di processi da utilizzare (puoi cambiarlo a seconda delle tue esigenze)
    num_processes = min(cpu_count(), len(keys))

    with Manager() as manager:
        # Inizializza una lista condivisa
        shared_dict = manager.dict()

        lock = Lock()  # Crea un Lock globale

        # Inizializza il dizionario condiviso con il lock
        initialize_shared_dict(lock, shared_dict)

        # Avvia i processi in parallelo
        with Pool(processes=num_processes, initializer=initialize_lock, initargs=(lock,)) as pool:
            results = pool.starmap(recordLinkageFromBlocking, [(key, shared_dict) for key in keys])

        shared_dict = dict(shared_dict)

    with open('recordLinkage/outcome_namet.json', 'w') as output:
        json.dump(shared_dict, output, indent=4)

    # Concatena i risultati ottenuti da ogni processo
    final_df = pd.concat(results, ignore_index=True)
    
    # Salva il dataframe finale in un file CSV
    final_df.to_csv("recordLinkage/record_linkage_name.csv", index=False)