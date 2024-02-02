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
accepted_att = []
unused_list = []
rows_to_write = []

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
    
    matchingClass.checkMatching(matches, matches_list, accepted_att)

    df2 = pd.read_csv(os.path.join("documents/"+file2), nrows=0)
    header2 = df2.columns.tolist()

    if i==0:
        df1 = pd.read_csv(os.path.join("documents/"+file1), nrows=0)
        header1 = df1.columns.tolist()
        for col1 in header1 :
            if(col1 not in accepted_att) :
                unused_list.append(col1)

    for col2 in header2 :
        if(col2 not in accepted_att) :
            unused_list.append(col2)

    print("matches_list:")
    print(matches_list)
    print("accepted_att:")
    print(accepted_att)
    print("unused_att:")
    print(unused_list)

    #PROBLEMA: SCHEMA_MATCHING_FILE E UNUSED_FILE VENGONO SOVRASCRITTI
    #CONTROLLARE SE è GIà PRESENTE UNA COLONNA IN SCHEMA MATCHING

    with open("documents/"+file2, 'r') as file:
        csv_reader = csv.DictReader(file)

        j=0
        for rowNum, row in enumerate(csv_reader):
            dati_attributo = []
            if i==0:
                for att in accepted_att:
                    if att in row:
                        dati_attributo.append(row[att])
                rows_to_write.append(dati_attributo)
                print("rows_to_write[j]")
                print(rows_to_write[j])
            else:
                for att in accepted_att:
                    if att in row:
                        print("rows_to_write[j]")
                        print(rows_to_write[j])
                        rows_to_write[j].append(row[att])
                        print("rows_to_write[j]")
                        print(rows_to_write[j])
            j+=1
            if rowNum == 11:
                break

with open("schema_matching_file.csv", 'w') as fileMatching: 
    writer = csv.writer(fileMatching, delimiter=',')
    writer.writerow(matches_list)
    writer.writerows(rows_to_write)
    
fileMatching.close()

       # writer.writerows(rows_to_write) DA METTERE DOPO
            
    # with open('unused_file.csv', "w", newline='') as unused_file:
    #     writer = csv.writer(unused_file, delimiter=',')
    #     writer.writerow(unused_list)

    #     with open('documents/ft.csv', 'r') as file:
    #         with open('documents/DDD-cbinsight.com.csv', 'r') as file2:
    #             csv_reader = csv.DictReader(file)
    #             csv_reader2 = csv.DictReader(file2)

    #             rows_to_write = []
    #             rowNum = 0
    #             for row1, row2 in zip(csv_reader, csv_reader2):

    #                 dati_attributo = []
                    
    #                 print(unused_list)
    #                 for att in unused_list:
    #                     try:
    #                         dati_attributo.append(row1[att])
    #                     except Exception as e :
    #                         dati_attributo.append(row2[att])
                        
    #                 print(dati_attributo)
    #                 rows_to_write.append(dati_attributo)

    #                 rowNum +=1
    #                 if rowNum == 11:  # Change the condition to >= 9 for 10 rows
    #                     break

    #             writer.writerows(rows_to_write)

    # match_file.close()
    # unused_file.close()
