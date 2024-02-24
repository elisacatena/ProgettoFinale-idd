import recordlinkage as rl
import recordlinkage
import pandas as pd
from recordlinkage.index import Full
import matplotlib.pyplot as plt
import seaborn as sns
import json
from sklearn.model_selection import KFold, cross_val_score
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_validate, train_test_split
from sklearn import metrics
from sklearn.metrics import classification_report
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier, plot_tree
from sklearn.metrics import confusion_matrix, accuracy_score, precision_score, recall_score, f1_score
from sklearn.preprocessing import LabelEncoder

def logisticRegressionStats(df) :

    # df = pd.read_csv('stats/testStats.csv', encoding='latin-1')
    # df = df.drop(df.columns[0], axis=1)
    print(df)

    df_modificato = pd.DataFrame()
    for col in df.columns :

        if col == 'outcome':
            continue
        df[col] = df[col].fillna('Unknown')
        label_encoder = LabelEncoder()
        categorie_encoded = label_encoder.fit_transform(df[col])
        df_modificato[col] = categorie_encoded

    print(df_modificato)
    sc_X = StandardScaler()
    x =  pd.DataFrame(sc_X.fit_transform(df_modificato,), columns=df_modificato.columns)

    df['outcome'] = df['outcome'].astype(int)
    y = df['outcome'] # set della variabile target
    print(x)
    # 80% del dataset costituisce il training set 
    x_train, x_tmp, y_train, y_tmp = train_test_split(x,y, train_size=0.7, random_state=42)

    # 50% validation set
    # 50% test set
    x_test, x_val, y_test, y_val = train_test_split(x_tmp, y_tmp, test_size=0.5, random_state=42)

    svc_model = SVC()
    svc_model.fit(x_train, y_train)

    # Tentativi con diversi iperparamentri
    # kernel_types = ['linear', 'rbf', 'poly']
    kernel_types = ['rbf']

    # C = iperparametro che misura l’importanza degli errori 
    # di classificazione rispetto all’ampiezza del margine
    C_values = [10]

    best_accuracy = 0
    best_svm = None

    for kernel in kernel_types:
        for C in C_values:
            svc_model = SVC(kernel=kernel, C=C)
            svc_model.fit(x_train, y_train)
            
            # Calcoliamo l'accuracy usando il validation set
            accuracy = metrics.accuracy_score(y_val, svc_model.predict(x_val))
            print(f"Validation Accuracy (kernel={kernel}, C={C}): {accuracy}")
            
            if accuracy > best_accuracy:
                best_accuracy = accuracy
                best_svm = svc_model

    print('PREDIZIONE SU TEST SET')
    svc_pred = best_svm.predict(x_test)
    accuracy = metrics.accuracy_score(y_test, svc_pred)
    print("Accuracy Score =", accuracy)
    matrix = confusion_matrix(y_test, svc_pred)
    # print(matrix)
    print(classification_report(y_test,svc_pred))
    sns.heatmap(matrix.astype(int),annot = True, cmap="Blues", fmt='0.0f')
    plt.show()

    print('PREDIZIONE SU TESTSTATS')
    df_test = pd.read_csv('stats/testStats.csv', encoding='latin-1')
    df_test_modificato = pd.DataFrame()
    for col in df_test.columns :
        if col == 'outcome':
            continue
        df_test[col] = df_test[col].fillna('Unknown')
        label_encoder = LabelEncoder()
        categorie_encoded = label_encoder.fit_transform(df_test[col])
        df_test_modificato[col] = categorie_encoded

    print(df_test_modificato)
    sc_X = StandardScaler()
    x =  pd.DataFrame(sc_X.fit_transform(df_test_modificato,), columns=df_test_modificato.columns)

    df_test['outcome'] = df_test['outcome'].astype(int)
    y_test = df_test['outcome'] # set della variabile target
    x_test = df_test_modificato
    svc_pred = best_svm.predict(x_test)
    accuracy = metrics.accuracy_score(y_test, svc_pred)
    print("Accuracy Score =", accuracy)
    matrix = confusion_matrix(y_test, svc_pred)
    # print(matrix)
    print(classification_report(y_test,svc_pred))
    sns.heatmap(matrix.astype(int),annot = True, cmap="Blues", fmt='0.0f')
    plt.show()


if __name__ == "__main__":

    with open('recordLinkage/outcome_test.json', 'r') as file:
        data = json.load(file)

    # dizionario = {}
    # for key in data:
    #     for entry in data[key]:
    #         dizionario['name'] = dizionario.get('name', []) + [entry['name']]
    #         dizionario['founded'] = dizionario.get('founded', []) + [entry['founded']]
    #         dizionario['outcome'] = dizionario.get('outcome', []) + [entry['outcome']]
    df = pd.DataFrame(data = data)

    # df.to_csv('stats/statsFile.csv', encoding='latin-1')

    logisticRegressionStats(df)