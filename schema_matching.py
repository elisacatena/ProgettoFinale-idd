import os
import pandas as pd
from valentine import valentine_match, valentine_metrics
from valentine.algorithms import Coma
import pprint as pp
import csv

class SchemaMatching:

    def matchingWithComa(self, file1, file2):
        # Load data using pandas
        d1_path = os.path.join(file1)
        d2_path = os.path.join(file2)
        df1 = pd.read_csv(d1_path, nrows=10, encoding='latin-1')
        df2 = pd.read_csv(d2_path, nrows=10, encoding='latin-1')
        
        # Instantiate matcher and run
        matcher = Coma(use_instances=True, java_xmx='8192m')
        matches = valentine_match(df1, df2, matcher)
        
        pp.pprint(matches)
        return matches
        # If ground truth available valentine could calculate the metrics
        # ground_truth = [('Name', 'name'),
        #                 ('Country', 'country'),
        #                 ('Founding Year', 'founded'),
        #                 ('Sector', 'industry')]
        
        # metrics = valentine_metrics.all_metrics(matches, ground_truth)
            
        # pp.pprint(metrics)
    
    def checkMatching(self, matches, matches_list, accepted_att, unused_list, unused_rows_to_write):
        for ((table1, att1), (table2, att2)) in matches:
            print('\nMatch: ' + att1 + ' - ' + att2 + ' -> '+ str(matches.get(((table1, att1), (table2, att2)))))
            print('Vuoi accettare? (y/n)')
            response = input()
            while True:
                if response == 'y':
                    print('Associa un nome al match: ')
                    name_match = input()
                    if os.stat("unused_file.csv").st_size != 0:
                        data = pd.read_csv('unused_file.csv', nrows=1, encoding='latin-1') 
                        if not data.empty and att1 in unused_list:
                            data.drop(att1, inplace=True, axis=1) 
                            data.to_csv("unused_file.csv", index=False, encoding='latin-1')
                            index = unused_list.index(att1)
                            print("index")
                            print(index)
                            print(unused_rows_to_write[0][index])
                            unused_list.remove(att1)
                            unused_rows_to_write[0].remove(unused_rows_to_write[0][index])

                    if(name_match in matches_list) :
                        print("Attributo già inserito")
                        break
                    matches_list.append(name_match)
                    accepted_att.append(att1)
                    accepted_att.append(att2)
                    break
                elif response == 'n':
                    break
                else:
                    print('Risposta non valida')
                    response = input()
    
    def finalCheck(self, matches_list, unused_list, unused_rows_to_write):
        for att in matches_list :
            stringhe_con_sottostringa = [stringa for stringa in unused_list if att[:3].lower() in stringa.lower()]
            if len(stringhe_con_sottostringa)>0 :
                print('Ecco i match: ' + att + ' -> ' + str(stringhe_con_sottostringa))
                print('Posso rimuovere? y/n')
                response = input()
                if response == 'y' :
                    deleted = stringhe_con_sottostringa[0]
                    if len(stringhe_con_sottostringa)>1 :
                        while True :
                            print('Quale vuoi eliminare? (0 per uscire)')
                            deleted = input()
                            if deleted == '0' :
                                break
                            index = unused_list.index(deleted) 
                            unused_list.remove(deleted)
                            unused_rows_to_write[0].remove(unused_rows_to_write[0][index])
                    else:
                        index = unused_list.index(deleted) 
                        unused_list.remove(deleted)
                        unused_rows_to_write[0].remove(unused_rows_to_write[0][index])
                else:
                    continue