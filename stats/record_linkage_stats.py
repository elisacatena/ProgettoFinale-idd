import recordlinkage as rl
import recordlinkage
import pandas as pd
from recordlinkage.index import Full
import matplotlib.pyplot as plt
import seaborn as sns

class recordLinkageStats :

    def logisticRegressionStats(self, matches, candidate_links, features) :

        # use the Logistic Regression Classifier
        # this classifier is equivalent to the deterministic record linkage approach
        intercept = -5
        coefficients = [5.0, 5.0]

        print("Deterministic classifier")
        print("intercept", intercept)
        print("coefficients", coefficients)

        logreg = rl.LogisticRegressionClassifier(coefficients=coefficients, intercept=intercept)
        X = features.values  # Matrice delle features
        y = matches.index.get_level_values(0) # Target binario (0 o 1) indicante se il record deve essere considerato come duplicato o no

        # Suddivisione dei dati in set di addestramento e test
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Creazione del modello di regressione logistica
        model = LogisticRegression()

        # Addestramento del modello
        model.fit(X_train, y_train)

        # Predizione su dati di test
        predictions = model.predict(X_test)

        # Valutazione del modello
        accuracy = model.score(X_test, y_test)
        print("Accuracy:", accuracy)
        links = logreg.predict(features)

        print(len(links), "links/matches")

        # return the confusion matrix
        conf_logreg = rl.confusion_matrix(matches, links, len(candidate_links))
        print("confusion matrix")
        # confusion_df = pd.DataFrame(conf_logreg, columns=['Predetto come non collegato', 'Predetto come collegato'], 
        #                             index=['Non collegato', 'Collegato'])

        # Creare una heatmap per visualizzare la matrice di confusione
        # plt.figure(figsize=(8, 6))
        # sns.heatmap(confusion_df, annot=True, fmt="d", cmap="Blues")
        # plt.title("Matrice di Confusione")
        # plt.xlabel("Predetto")
        # plt.ylabel("Reale")
        # plt.show()
        # compute the F-score for this classification
        fscore = rl.fscore(conf_logreg)
        print("fscore", fscore)
        recall = rl.recall(matches, links)
        print("recall", recall)
        precision = rl.precision(matches, links)
        print("precision", precision)