import recordlinkage
import pandas as pd

class RecordLinkageClass:

    def blockingMethod(self) :

        dfA = pd.read_csv('schema_matching_file.csv', encoding='latin-1')

        # Indexation step
        indexer = recordlinkage.Index()
        indexer.full()
        indexer.block(left_on=['name', 'founded'])
        candidate_links = indexer.index(dfA)

        # Comparison step
        compare_cl = recordlinkage.Compare()

        # compare_cl.exact("name", "name", label="name")
        # compare_cl.exact("founded", "founded", label="founded")
        compare_cl.string("name", "name", threshold=0.85, label="name")

        features = compare_cl.compute(candidate_links, dfA)
        matches = features[features.sum(axis=1) > 0]
        for couple in matches.index:
            if couple[0] >= couple[1]:
                matches.drop(couple, inplace=True)
        # print(matches)
        # Classification step
        # features_numerici = features.apply(pd.to_numeric, errors='coerce')

        # # Esegui la somma delle righe
        # somma_righe = features_numerici.sum(axis=1)

        # # Esegui value_counts() sulla somma delle righe
        # conteggio_valori = somma_righe.value_counts().sort_index(ascending=False)

        # print(conteggio_valori)