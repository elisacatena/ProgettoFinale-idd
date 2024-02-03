import pandas as pd

class ConvertToCSVFileClass :

    def convertFile(self, file) :

        df = None
        with open('documents/'+file, encoding='utf-8') as inputfile:
            if(file.endswith('.json') or file.endswith('.jsonl')):
                df = pd.read_json(inputfile)

            else :
                df = pd.read_excel(inputfile)

        df.to_csv('documents/'+file+'.csv', encoding='utf-8', index=False)

        return df
