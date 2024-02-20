import recordlinkage
import pandas as pd
from recordlinkage.preprocessing import clean
from recordlinkage.index import Full

class RecordLinkageClass:

    def recordLinkageMethod(self, dfBlock) :

        indexer = recordlinkage.Index()   
        indexer.add(Full())     
        candidate_links = indexer.index(dfBlock)

        # Comparison step
        compare_cl = recordlinkage.Compare()

        # compare_cl.string("industry", "industry",threshold=0.85, label="industry")
        # compare_cl.string("rank", "rank", threshold=0.85, label="rank")
        compare_cl.string("name", "name", threshold=0.85, label="name")

        features = compare_cl.compute(candidate_links, dfBlock)
        matches = features[features.sum(axis=1) > 0]
        # for couple in matches.index:
        #     if couple[0] >= couple[1]:
        #         matches.drop(couple, inplace=True)
        merged_data = pd.DataFrame()
        for (id1, id2) in matches.index:
            dfBlock.loc[[id1]].fillna(dfBlock.loc[[id2]], inplace=True)
            merged_df = pd.concat([dfBlock.loc[[id1]], dfBlock.loc[[id2]]]).groupby('name', as_index=False).first()
            merged_data = pd.concat([merged_data, merged_df])

 
# Unione dei due DataFrame basata sulla colonna 'name'

        print(merged_data)

