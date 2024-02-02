import os
import pandas as pd
from valentine import valentine_match, valentine_metrics
from valentine.algorithms import Coma
import pprint as pp
import csv
 
# Load data using pandas
d1_path = os.path.join('documents', 'ft.csv')
d2_path = os.path.join('documents','DDD-cbinsight.com.csv')
df1 = pd.read_csv(d1_path, nrows=10)
df2 = pd.read_csv(d2_path, nrows=10)
 
# Instantiate matcher and run
matcher = Coma(use_instances=True)
matches = valentine_match(df1, df2, matcher)
 
pp.pprint(matches)
 
# If ground truth available valentine could calculate the metrics
ground_truth = [('Name', 'name'),
                ('Country', 'country'),
                ('Founding Year', 'founded'),
                ('Sector', 'industry')]
 
metrics = valentine_metrics.all_metrics(matches, ground_truth)
    
pp.pprint(metrics)

matches_list = []
accepted_att = []
unused_list = []
unused_list_temp = []

for ((table1, att1), (table2, att2)) in matches:
    print('\nMatch: ' + att1 + ' - ' + att2 + ' -> '+ str(matches.get(((table1, att1), (table2, att2)))) + '\n')
    print('Vuoi accettare? (y/n)')
    response = input()
    while True:
        if response == 'y':
            print('Associa un nome al match: ')
            name_match = input()
            matches_list.append(name_match)
            accepted_att.append(att1)
            accepted_att.append(att2)
            break
        elif response == 'n':
            break
        else:
            print('Risposta non valida')
            response = input()

df1 = pd.read_csv(d1_path, nrows=0)
header1 = df1.columns.tolist()
df2 = pd.read_csv(d2_path, nrows=0)
header2 = df2.columns.tolist()

for col1 in header1 :
    if(col1 not in accepted_att) :
        unused_list.append(col1)

for col2 in header2 :
    if(col2 not in accepted_att) :
        unused_list.append(col2)


with open('schema_matching_file.csv', "w", newline='') as match_file:
    writer = csv.writer(match_file, delimiter=',')
    writer.writerow(matches_list)
    with open('documents/ft.csv', 'r') as file:
        csv_reader = csv.DictReader(file)
        rows_to_write = []

        for rowNum, row in enumerate(csv_reader):
            dati_attributo = [row[att] for att in accepted_att if att in row]
            print(dati_attributo)
            rows_to_write.append(dati_attributo)

            if rowNum == 11:
                break

        writer.writerows(rows_to_write)
        
with open('unused_file.csv', "w", newline='') as unused_file:
    writer = csv.writer(unused_file, delimiter=',')
    writer.writerow(unused_list)

    with open('documents/ft.csv', 'r') as file:
        with open('documents/DDD-cbinsight.com.csv', 'r') as file2:
            csv_reader = csv.DictReader(file)
            csv_reader2 = csv.DictReader(file2)

            rows_to_write = []
            rowNum = 0
            for row1, row2 in zip(csv_reader, csv_reader2):

                dati_attributo = []
                
                print(unused_list)
                for att in unused_list:
                    try:
                        dati_attributo.append(row1[att])
                    except Exception as e :
                        dati_attributo.append(row2[att])
                    
                print(dati_attributo)
                rows_to_write.append(dati_attributo)

                rowNum +=1
                if rowNum == 11:  # Change the condition to >= 9 for 10 rows
                    break

            writer.writerows(rows_to_write)

match_file.close()
unused_file.close()








    




    
