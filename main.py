import os
import pandas as pd
from valentine import valentine_match, valentine_metrics
from valentine.algorithms import Coma
import pprint as pp
import csv
from schema_matching import SchemaMatching

matchingClass = SchemaMatching()

i=0
matches_list = []
unused_list = []
rows_to_write = []
unused_rows_to_write = []

prova = open('unused_file.csv', "w", newline='')
prova.close()

files = [nome_file for nome_file in os.listdir("documents") if os.path.isfile(os.path.join("documents", nome_file))]
for i in range(len(files)):
    file1, file2 = None, None
    if i==0:
        file1 = files[i]
        file2 = files[i+1]
        matches = matchingClass.matchingWithComa("documents/"+file1, "documents/"+file2)
    elif i==1:
        continue
    else:
        file2 = files[i]
        matches = matchingClass.matchingWithComa("unused_file.csv", "documents/"+file2)
    
    accepted_att = []
    matchingClass.checkMatching(matches, matches_list, accepted_att, unused_list, unused_rows_to_write)

    df2 = pd.read_csv(os.path.join("documents/"+file2), nrows=1, encoding='latin-1')
    header2 = df2.columns.tolist()
    valori_att = []
    dati_attributo = []

    if i==0:
        df1 = pd.read_csv(os.path.join("documents/"+file1), nrows=1, encoding='latin-1')
        header1 = df1.columns.tolist()
        
        for att in header1 :
            if(att not in accepted_att) :
                unused_list.append(att)
            
        for att in accepted_att :
            if att in header1:
                valori_att.append(df1[att].values[0])
            
        for att in unused_list :
            try:
                dati_attributo.append(df1[att].values[0])
            except Exception as e :
                dati_attributo.append(df2[att].values[0])

    else :           
        for att2 in header2 :
            if(att2 not in accepted_att) :
                unused_list.append(att2)
        i = 0
        for att2 in accepted_att : 
            if att2 in header2 and i%2 == 1:
                valori_att.append(df2[att2].values[0])
            i += 1
 
        for att in unused_list :
            if att in header2:
                dati_attributo.append(df2[att].values[0])

    if len(rows_to_write) > 0:
        rows_to_write[0].extend(valori_att)
    else:
        rows_to_write.append(valori_att)
    if len(unused_rows_to_write) > 0 :
        unused_rows_to_write[0].extend(dati_attributo)
    else :
        unused_rows_to_write.append(dati_attributo)

    with open('unused_file.csv', "w", encoding='latin-1', newline='') as unused_file:
        writer = csv.DictWriter(unused_file, unused_list, delimiter=',')
        writer.writeheader() 
        row = {}
        for att, val in zip(unused_list, unused_rows_to_write[0]) :
            row[att]=val
        writer.writerow(row)
with open("schema_matching_file.csv", 'w', encoding='latin-1',newline='') as fileMatching: 
    writer = csv.DictWriter(fileMatching, matches_list, delimiter=',')
    writer.writeheader()

    row = {}
    for att, val in zip(matches_list, rows_to_write[0]) :
        row[att]=val
    writer.writerow(row)

fileMatching.close()    
unused_file.close()
