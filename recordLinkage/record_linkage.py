import recordlinkage
import pandas as pd
from recordlinkage.preprocessing import clean
from recordlinkage.index import Full
import json
import multiprocessing

class RecordLinkageClass:

    def recordLinkageMethod(self, dfBlock, fileNameBlocking, chiave, shared_dict):
        indexer = recordlinkage.Index()   
        indexer.add(Full())     
        candidate_links = indexer.index(dfBlock)

        # Comparison step
        compare_cl = recordlinkage.Compare()

        compare_cl.string("name", "name", threshold=0.3, label="name")
        # compare_cl.string("industry", "industry", threshold=0.85, label="industry")

        features = compare_cl.compute(candidate_links, dfBlock)
        matches = features[features.sum(axis=1) > 0]
        non_matches = features[features.sum(axis=1) == 0]
        resultBlock = pd.DataFrame()

        index_list = []
        i = 0

        with open(fileNameBlocking, 'r') as file:
            data = json.load(file)
        
        for (id1, id2) in matches.index:
            # entry1 = data[chiave][id1]
            # entry2 = data[chiave][id2]

            # entry1['outcome'] = '1'
            # entry2['outcome'] = '1'
            print(id1, id2)
            if id1 in index_list or id2 in index_list:
                continue
            else:
                for (i1, i2) in matches.index:
                    if i1 in index_list or i2 in index_list:
                        continue
                    if not i1 == id1 and not i2 == id1:
                        continue
                    else:
                        if i1 == id1:
                            founded1 = dfBlock.loc[i1]['founded']
                            founded2 = dfBlock.loc[i2]['founded']
                            
                            if founded2 == '' or (founded1 != '' and founded2 != '' and abs(int(founded1)-int(founded2)) <= 30) :
                                first_elem = dfBlock.loc[[i1]]
                                for col in first_elem.columns:
                                    # if pd.isnull(dfBlock.loc[id1][col]) and pd.notnull(dfBlock.loc[i2][col]):
                                    if dfBlock.loc[id1][col] != "" and dfBlock.loc[i2][col] == "":
                                        dfBlock.loc[i2][col]  = dfBlock.loc[id1][col]
                        else:
                            founded1 = dfBlock.loc[i1]['founded']
                            founded2 = dfBlock.loc[i2]['founded']
                            if founded1 == '' or (founded1 != '' and founded2 != '' and abs(int(founded1)-int(founded2)) <= 30):
                                first_elem = dfBlock.loc[[i2]]
                                for col in first_elem.columns:
                                    if dfBlock.loc[id1][col] != "" and dfBlock.loc[i1][col] == "":
                                        dfBlock.loc[i1][col] = dfBlock.loc[id1][col]
                
                dfBlock = dfBlock.drop([id1])  
                index_list.append(id1)
                if i <= 5000:
                    shared_dict["name1"] = shared_dict.get("name1", []) + [data[chiave][id1]['name']]
                    shared_dict["name2"] = shared_dict.get("name2", []) + [data[chiave][id2]['name']]
                    shared_dict["outcome"] = shared_dict.get("outcome", []) + ["1"]

        i = 0
        for (id1, id2) in non_matches.index:
            if i == 5001:
                break
            shared_dict["name1"] = shared_dict.get("name1", []) + [data[chiave][id1]['name']]
            shared_dict["name2"] = shared_dict.get("name2", []) + [data[chiave][id2]['name']]
            shared_dict["outcome"] = shared_dict.get("outcome", []) + ["0"]


        resultBlock = pd.concat([resultBlock, dfBlock], ignore_index=True)

    
                    

        # df = pd.DataFrame(data[chiave])
        # stats = recordLinkageStats()
        # stats.logisticRegressionStats(df)

        # with RecordLinkageClass.lock:
        #     with open('prova.json', 'w') as file:
        #         json.dump(data, file, indent=4)

        return resultBlock