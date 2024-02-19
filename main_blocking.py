import json
import pandas as pd
import multiprocessing as mp
import numpy as np
from record_linkage import RecordLinkageClass

def recordLinkageFromBlocking():

    recordLinkageObj = RecordLinkageClass()
    with open('sample.json', 'r') as file:
        data = json.load(file)
    
    for key in data.keys():
        print(len(data[key]))
        if len(data[key]) > 1 :
            dfBlock = pd.DataFrame(data[key])
            recordLinkageObj.recordLinkageMethod(dfBlock)


def transformString(name):
    nameLower = name.lower()
    result = ''.join(e for e in nameLower if e.isalnum())
    return result

def process_chunk(chunk):
    chunk.fillna('', inplace=True)
    name_dict = {}
    for _, row in chunk.iterrows():
        name = row['name']
        nameChanged = transformString(name)
        row_dict = row.to_dict()
        if nameChanged[:2] in name_dict:
            name_dict[nameChanged[:2]].append(row_dict)
        else:
            name_dict[nameChanged[:2]] = [row_dict]
    return name_dict

def merge_dicts(dict1, dict2):
    for key, value in dict2.items():
        if key in dict1:
            dict1[key] += value
        else:
            dict1[key] = value
    return dict1

def create_dictionary(filename):
    pool_size = mp.cpu_count()  # Imposta il numero di processi da utilizzare
    pool = mp.Pool(pool_size)
    manager = mp.Manager()
    return_dict = manager.dict()

    chunks = pd.read_csv(filename, chunksize=1000, encoding='latin-1')
    results = pool.map(process_chunk, chunks)

    for result in results:
        return_dict = merge_dicts(return_dict, result)

    pool.close()
    pool.join()

    return dict(return_dict)
def main():
    # filename = 'schema_matching_file.csv'
    # df = pd.read_csv(filename, encoding='latin-1')
    # print(df.isnull().sum())
    # name_dict = create_dictionary(filename)
    # # for name, rows in name_dict.items():
    # #     print(f"Name: {name}, Rows: {rows}")

    # print(len(name_dict.keys()))
    # with open("sample.json", "w") as outfile: 
    #     json.dump(name_dict, outfile, indent=4)

    recordLinkageFromBlocking()

if __name__ == "__main__":
    main()
