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
        matches = features[features.sum(axis=1) > 1]
        # for couple in matches.index:
        #     if couple[0] >= couple[1]:
        #         matches.drop(couple, inplace=True)
        print(matches)

