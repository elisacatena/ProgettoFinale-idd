from record_linkage import RecordLinkageClass
import json
import pandas as pd
from multiprocessing import Pool, cpu_count

def recordLinkageFromBlocking(key):
    recordLinkageObj = RecordLinkageClass()
    
    with open('blockingByName.json', 'r') as file:
        data = json.load(file)

    if len(data[key]) > 1:
        dfBlock = pd.DataFrame(data[key])
        dfBlock = dfBlock.drop(columns=['outcome'])
        return recordLinkageObj.recordLinkageMethod(dfBlock, 'blockingByName.json', key)

if __name__ == "__main__":
    with open('blockingByName.json', 'r') as file:
        data = json.load(file)

    keys = [key for key in data if len(data[key]) > 1]

    # Calcola il numero di processi da utilizzare (puoi cambiarlo a seconda delle tue esigenze)
    num_processes = min(cpu_count(), len(keys))

    # Avvia i processi in parallelo
    with Pool(processes=num_processes) as pool:
        results = pool.map(recordLinkageFromBlocking, keys)

    # Concatena i risultati ottenuti da ogni processo
    final_df = pd.concat(results, ignore_index=True)
    
    # Salva il dataframe finale in un file CSV
    final_df.to_csv("recordLinkage/record_linkage_name.csv", index=False)

