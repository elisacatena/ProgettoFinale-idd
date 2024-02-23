import recordlinkage
import pandas as pd
from recordlinkage.preprocessing import clean
from recordlinkage.index import Full

class RecordLinkageClass:

    def recordLinkageMethod(self, dfBlock):
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

        for (id1, id2) in matches.index:
            if id1 in index_list:
                continue
            else:
                merged_data = dfBlock.loc[[id1]]
                for (i1, i2) in matches.index:
                    if not i1 == id1 and not i2 == id1:
                        continue
                    else:
                        if i1 == id1:
                            first_elem = dfBlock.loc[[i1]]
                            for col in first_elem.columns:
                                if pd.isnull(dfBlock.loc[id1][col]) and pd.notnull(dfBlock.loc[i2][col]):
                                    dfBlock.loc[id1][col]  = dfBlock.loc[i2][col]

                        else:
                            first_elem = dfBlock.loc[[i2]]
                            for col in first_elem.columns:
                                if pd.isnull(dfBlock.loc[id1][col]) and pd.notnull(dfBlock.loc[i1][col]):
                                    dfBlock.loc[id1][col] = dfBlock.loc[i1][col]
                index_list.append(id1)
                resultBlock = pd.concat([resultBlock, dfBlock.loc[[id1]]], ignore_index=True)

        return resultBlock


            


