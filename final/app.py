import json
import os
from flask import Flask, jsonify, redirect, render_template, request, url_for
import pandas as pd
from valentine import valentine_match, valentine_metrics
from valentine.algorithms import Coma
from schema_matching import SchemaMatching
import csv

app = Flask(__name__, template_folder='templates')
global files 
files = [nome_file for nome_file in os.listdir("final/static/documents") if os.path.isfile(os.path.join("final/static/documents", nome_file))]
global coma 
coma = Coma(use_instances=True, java_xmx='1024m')
global i
i=0
global matchingClass 
matchingClass= SchemaMatching()
global matches_dict
matches_dict = {}
global unused_dict
unused_dict = {}

def updateUnusedFile():
    global unused_dict
    with open('final/static/unused.csv', "w", encoding='latin-1', newline='') as unused_file:
        writer = csv.DictWriter(unused_file, unused_dict.keys(), delimiter=',')
        writer.writeheader() 
        row = {}
        for key, value in unused_dict.items():            
            row[key]=value[0][1]
        writer.writerow(row)         

def convertFile(file) :
    df = None
    with open(file, encoding='utf-8') as inputfile:
        if(file.endswith('.json')):
            try:
                df = pd.read_json(inputfile)
            except Exception as e :
                json_data = json.load(file)
                df = pd.json_normalize(json_data)
            nome_senza_estensione = os.path.splitext(os.path.basename('final/static/jsons/'+file))[0]
            df.to_csv('final/static/documents/' + nome_senza_estensione + '.csv', encoding='utf-8', index=False)
        elif(file.endswith('.xls')):
            df = pd.read_excel(inputfile)
            nome_senza_estensione = os.path.splitext(os.path.basename('final/static/xls/'+file))[0]
            df.to_csv('final/static/documents/' + nome_senza_estensione + '.csv', encoding='utf-8', index=False)
        else:
            file.save('final/static/documents/'+file)


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_upload_url')
def get_upload_url():
    # Qui puoi generare dinamicamente l'URL per la pagina di upload o recuperarlo da qualche altra fonte
    upload_url = "/upload.html"
    return jsonify({'upload_url': upload_url})

@app.route('/get_matching_url')
def get_matching_url():
    # Qui puoi generare dinamicamente l'URL per la pagina di upload o recuperarlo da qualche altra fonte
    matching_url = "/matching.html"
    return jsonify({'matching_url': matching_url})

@app.route('/upload.html')
def upload_page():
    return render_template('uploadFiles.html', uploaded=False)

@app.route('/matching.html')
def matching_page():
    global i
    global files
    global coma
    global matchingClass
    theEnd = False
    if i==0 :
        matches = matchingClass.matchingWithComa("final/static/documents/"+files[i], "final/static/documents/"+files[i+1], coma)
        i+=1
        print('primo')
        print(i)
    elif i>0 and i<len(files):
        if i == 1 :
            i+=1
        matches = matchingClass.matchingWithComa("final/static/unused.csv", "final/static/documents/"+files[i], coma)
        i+=1
    else :
        matches=None
        theEnd = True
    return render_template('matching.html', data_dict=matches, theEnd = theEnd)

@app.route('/submit', methods=['POST'])
def submit():
    global i
    global matches_dict
    global unused_dict
    print('secondo')
    print(i)
    df = pd.read_csv("final/static/documents/"+files[i-1], encoding='latin-1')
    header = df.columns.tolist()
    print(header)

    header1 = []
    if i == 1:
        df1 = pd.read_csv("final/static/documents/"+files[i], encoding='latin-1')
        header1 = df1.columns.tolist()
        print(header1)


    if request.method == 'POST':
        checkbox_values = {}
        text_input_values = {}

        # Itera attraverso i dati del modulo
        for key, value in request.form.items():
            if key.startswith('checkbox_'):
                checkbox_values[key] = value
            elif key.startswith('text_input_') and value:
                text_input_values[key] = value

        for key, value in text_input_values.items() :
            index = key.split('_')[2]
            att = checkbox_values.get('checkbox_' + index )
            att_list = att.split('-')
            print(att_list[0])
            print(att_list[1])
            if i == 1:
                print('qua')
                matches_dict[value.lower()] = matches_dict.get(value.lower(), []) + [(files[i], att_list[1])] + [(files[0], att_list[0])]
                print(matches_dict)
                header.remove(att_list[0])
                header1.remove(att_list[1])
            else:
                print('qui')
                matches_dict[value.lower()] = matches_dict.get(value.lower(), []) + [(files[i-1], att_list[1])]
                header.remove(att_list[1])

        for col in header :
            unused_dict[col] = unused_dict.get(col, []) + [(files[i-1], df[col].values[0])]
        for col in header1 :
            unused_dict[col] = unused_dict.get(col, []) + [(files[i], df1[col].values[0])]
        

        print(unused_dict)
        # print(matches_dict)
        updateUnusedFile()

        return redirect(url_for('matching_page'))
    else:
        return "Errore: richiesta non valida"


@app.route('/button1_clicked', methods=['POST'])
def button1_clicked():
    # Logica per gestire il click del bottone 1
    print("Bottone 1 cliccato")
    # Puoi inserire qui la logica aggiuntiva
    return render_template(url_for('uploadFiles'))

@app.route('/button2_clicked', methods=['POST'])
def button2_clicked():
    # Logica per gestire il click del bottone 2
    print("Bottone 2 cliccato")
    # Puoi inserire qui la logica aggiuntiva

    return '', 204  # 204 No Content


@app.route('/uploader', methods = ['GET', 'POST'])
def upload_file():
   if request.method == 'POST':
        files = request.files.getlist('file')
        for file in files:
            if(file.filename.endswith('.json')):
                file.save('final/static/jsons/'+file.filename)
                convertFile('final/static/jsons/'+file.filename)
            else:
                file.save('final/static/documents/'+file.filename)
        return render_template('uploadFiles.html', uploaded = True)


    

    

if __name__ == '__main__':
    app.run(debug=True)
