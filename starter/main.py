import os
import pandas as pd
from valentine.algorithms import Coma
import csv
from schema_matching import SchemaMatching
from starter.convert_toCSV import ConvertToCSVFileClass
from recordLinkage.record_linkage import RecordLinkageClass
j=0

if j==1:
    matchingClass = SchemaMatching()

    matches_list = []
    unused_list = []
    rows_to_write = []
    unused_rows_to_write = []
    matches_dict = {}
    unused_dict = {}

    prova = open('unused_file.csv', "w", newline='')
    prova.close()
    convertClass = ConvertToCSVFileClass()

    # files = [nome_file for nome_file in os.listdir("documents") if os.path.isfile(os.path.join("documents", nome_file))]

    # for file in files:
    #     convertClass.convertFile("documents/"+file)

    files2 = [nome_file for nome_file in os.listdir("documents") if os.path.isfile(os.path.join("documents", nome_file))]

    coma = Coma(use_instances=True, java_xmx='1024m')

    i=0
    for i in range(len(files2)):
        file1, file2 = None, None
        print(files2[i])
        if i==0:
            file1 = files2[i]
            file2 = files2[i+1]
            print(file2)
            matches = matchingClass.matchingWithComa("documents/"+file1, "documents/"+file2, coma)
        elif i==1:
            continue
        else:
            file1 = None
            file2 = files2[i]
            print(file2)
            matches = matchingClass.matchingWithComa("unused_file.csv", "documents/"+file2, coma)
        accepted_att = []
        
        matchingClass.checkMatching(matches, matches_list, accepted_att, unused_list, unused_rows_to_write, matches_dict, file1, file2, unused_dict)

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
                    unused_dict[att.lower()] = unused_dict.get(att.lower(), []) + [(file1, att)]
                
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
                    unused_dict[att2.lower()] = unused_dict.get(att2.lower(), []) + [(file2, att2)]
                    
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


        # row = {}
        # for att, val in zip(matches_list, rows_to_write[0]) :
        #     row[att]=val
        # writer.writerow(row)

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
                        print('Quale vuoi eliminare? (0 per uscire, all per eliminarli tutti)')
                        deleted = input()
                        if deleted == '0' :
                            break
                        elif deleted == 'all' :
                            for elem in stringhe_con_sottostringa:
                                matches_dict[att.lower()] = matches_dict.get(att.lower(), []) + unused_dict.get(elem.lower(), [])
                                unused_dict.pop(elem.lower())
                                index = unused_list.index(elem) 
                                unused_list.remove(elem)
                                unused_rows_to_write[0].remove(unused_rows_to_write[0][index])
                            break
                        matches_dict[att.lower()] = matches_dict.get(att.lower(), []) + unused_dict.get(deleted.lower(), [])
                        unused_dict.pop(deleted.lower())
                        index = unused_list.index(deleted) 
                        unused_list.remove(deleted)
                        unused_rows_to_write[0].remove(unused_rows_to_write[0][index])
                else:
                    matches_dict[att.lower()] = matches_dict.get(att.lower(), []) + unused_dict.get(deleted.lower(), [])
                    unused_dict.pop(deleted.lower())
                    index = unused_list.index(deleted) 
                    unused_list.remove(deleted)
                    unused_rows_to_write[0].remove(unused_rows_to_write[0][index])
            else:
                continue

    print('Rimangono ancora questi: ' + str(unused_list))
    print('matches_dict')
    print(matches_dict)


    with open("schema_matching_file.csv", 'w', encoding='latin-1',newline='') as fileMatching: 
        # writer = csv.DictWriter(fileMatching, matches_list + [x.lower() for x in unused_list], delimiter=',')
        writer = csv.DictWriter(fileMatching, matches_list, delimiter=',')
        writer.writeheader()

    matchingClass.makeSchemaMatching(matches_list, matches_dict)

    fileMatching.close()    
    unused_file.close()

else:
    recordLinkageObj = RecordLinkageClass()
    recordLinkageObj.blockingMethod()
