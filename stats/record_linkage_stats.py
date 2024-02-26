from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix, classification_report, accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer
from nltk.stem import WordNetLemmatizer
import pandas as pd
import re
import json
from sklearn.feature_extraction.text import TfidfVectorizer

def logisticRegressionStats(df):
    
    data = pd.read_csv("stats/testStats.csv", encoding="latin-1")

    print(df)
    # Dividi il dataset in features (X) e target (y)
    X = df.drop(columns='outcome')  # Sostituisci con i nomi delle tue colonne testuali
    y = df['outcome']  # Sostituisci con il nome della tua colonna target
    print(X)
    # Converti le features testuali in una rappresentazione numerica utilizzando CountVectorizer
    # vectorizer = CountVectorizer()
    # X = vectorizer.fit_transform(X)
    tfidf_vectorizer = TfidfVectorizer()
    # X = tfidf_vectorizer.fit_transform(X)
    X = tfidf_vectorizer.fit_transform(X['name1'] + ' ' + X['name2'])
    print(X)

    # Dividi il dataset in insiemi di addestramento e test
    print("Shape of X:", X.shape)
    print("Shape of y:", y.shape)
    X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=0.7, random_state=42)

    # Inizializza e addestra il modello di regressione logistica
    model = RandomForestClassifier(n_estimators=10, random_state=0)    
    model.fit(X_train, y_train)

    # Effettua le previsioni sul dataset di test
    predictions = model.predict(X_test)

    # Valuta le prestazioni del modello
    accuracy = accuracy_score(y_test, predictions)
    print("Accuracy:", accuracy)

    # Ottieni un report dettagliato delle prestazioni del modello
    print("Classification Report:")
    print(classification_report(y_test, predictions))

    x_data = data.drop(columns='outcome')
    x_data = tfidf_vectorizer.transform(x_data['name1'] + ' ' + x_data['name2'])
    y_data = data['outcome']  # Sostituisci con il nome della tua colonna target
    predictions_data = model.predict(x_data)
    accuracy1 = accuracy_score(y_data, predictions_data)
    print("Accuracy:", accuracy1)
    # Ottieni un report dettagliato delle prestazioni del modello
    print("Classification Report:")
    print(classification_report(y_data, predictions_data))




def preprocessing(df):
    stemmer = WordNetLemmatizer()
    df_new = pd.DataFrame()
    documents = []
    documents2 = []

    for index, row in df.iterrows():
        # Rimuovi i caratteri speciali
        document = re.sub(r'\W', ' ', str(row['name1']))
        document2 = re.sub(r'\W', ' ', str(row['name2']))

        # Rimuovi tutti i singoli caratteri
        document = re.sub(r'\s+[a-zA-Z]\s+', ' ', document)
        document2 = re.sub(r'\s+[a-zA-Z]\s+', ' ', document2)

        # Rimuovi i singoli caratteri dall'inizio
        document = re.sub(r'\^[a-zA-Z]\s+', ' ', document)
        document2 = re.sub(r'\^[a-zA-Z]\s+', ' ', document2)

        # Sostituisci più spazi con uno spazio singolo
        document = re.sub(r'\s+', ' ', document, flags=re.I)
        document2 = re.sub(r'\s+', ' ', document2, flags=re.I)

        # Rimuovi il prefisso 'b'
        document = re.sub(r'^b\s+', '', document)
        document2 = re.sub(r'^b\s+', '', document2)

        # Converti in minuscolo
        document = document.lower()
        document2 = document2.lower()

        # Lemmatization
        document = document.split()
        document = [stemmer.lemmatize(word) for word in document]
        document = ' '.join(document)
        document2 = document2.split()
        document2 = [stemmer.lemmatize(word) for word in document2]
        document2 = ' '.join(document2)

        documents.append(document)
        documents2.append(document2)

    # Add the target variable to the new DataFrame
    df_new['name1'] = documents
    df_new['name2'] = documents2
    df_new['outcome'] = df['outcome']  # Add the target variable
    return df_new


if __name__ == "__main__":
    with open('stats/outcome_name.json', 'r') as file:
        data = json.load(file)

    df = pd.DataFrame(data=data)
    # df_new = preprocessing(df)
    # print(df_new)
    logisticRegressionStats(df)
