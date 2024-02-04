import pandas as pd
import os
import json

class ConvertToCSVFileClass :

    def convertFile(self, file) :
        df = None
        with open(file, encoding='utf-8') as inputfile:
            if(file.endswith('.json')):
                try:
                    df = pd.read_json(inputfile)
                except Exception as e :
                    json_data = json.load(file)
                    df = pd.json_normalize(json_data)
                nome_senza_estensione = os.path.splitext(os.path.basename('documents/'+file))[0]
                df.to_csv('documents/' + nome_senza_estensione + '.csv', encoding='utf-8', index=False)
