import recordlinkage
import pandas as pd
from recordlinkage.preprocessing import clean
from recordlinkage.index import Full
from stats.record_linkage_stats import recordLinkageStats
import json


class RecordLinkageClass:

    def recordLinkageMethod(self, dfBlock, fileNameBlocking, chiave):
        indexer = recordlinkage.Index()   
        indexer.add(Full())     
        candidate_links = indexer.index(dfBlock)

        # Comparison step
        compare_cl = recordlinkage.Compare()

        compare_cl.string("name", "name", threshold=0.40, label="name")
        compare_cl.string("industry", "industry", threshold=0.85, label="industry")

        features = compare_cl.compute(candidate_links, dfBlock)
        matches = features[features.sum(axis=1) > 1]
        resultBlock = pd.DataFrame()

        index_list = []

        # !!!!!!!!!!!!!!!!!!!!!!!!!!!! CANCELLAAAAAAA !!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # VERIFICARE SE FOUNDED E' VUOTA O IN UN ARCO DI 50 ANNI -> MERGE
        # ALTRIMENTI SONO 2 COPPIE DIVERSE
        with open(fileNameBlocking, 'r') as file:
            data = json.load(file)

        for (id1, id2) in matches.index:
            entry1 = data[chiave][id1]
            entry2 = data[chiave][id2]

            entry1['outcome'] = '1'
            entry2['outcome'] = '1'
            print(id1,id2)
            if id1 in index_list or id2 in index_list:
                continue
            else:
                for (i1, i2) in matches.index:
                    if i1 in index_list or i2 in index_list:
                        continue
                    if not i1 == id1 and not i2 == id1:
                        continue
                    else:
                        print("else")
                        print(i1, i2)
                        if i1 == id1:
                            first_elem = dfBlock.loc[[i1]]
                            for col in first_elem.columns:
                                # if pd.isnull(dfBlock.loc[id1][col]) and pd.notnull(dfBlock.loc[i2][col]):
                                if dfBlock.loc[id1][col] != "" and dfBlock.loc[i2][col] == "":
                                    dfBlock.loc[i2][col]  = dfBlock.loc[id1][col]
                        else:
                            first_elem = dfBlock.loc[[i2]]
                            for col in first_elem.columns:
                                if dfBlock.loc[id1][col] != "" and dfBlock.loc[i1][col] == "":
                                    dfBlock.loc[i1][col] = dfBlock.loc[id1][col]
                dfBlock = dfBlock.drop([id1])  
                index_list.append(id1)
                resultBlock = pd.concat([resultBlock, dfBlock], ignore_index=True)
                
        with open(fileNameBlocking, 'w') as file:
            json.dump(data, file)
        stats = recordLinkageStats()
        stats.logisticRegressionStats(matches, candidate_links, features)

        return resultBlock


            


