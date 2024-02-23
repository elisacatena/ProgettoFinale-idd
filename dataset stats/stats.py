import os
import csv

directory = "final/static/documents"
files = [nome_file for nome_file in os.listdir(directory) if os.path.isfile(os.path.join(directory, nome_file))]

def computeStats(file_path):
    try:
        with open(file_path, 'r', encoding="latin-1") as file:
            reader = csv.reader(file)
            first_row = next(reader)  # Leggi la prima riga
            num_cols = float(len(first_row))
            num_nulls = 0.0
            row_count = 0.0
            nulls_per_column = [0.0] * int(num_cols)

            for row in reader:
                row_count += 1
                for i, value in enumerate(row):
                    if not value.strip():  # Se il valore è una stringa vuota o solo spazi bianchi
                        num_nulls += 1.0
                        nulls_per_column[i] += 1.0

        return num_cols, row_count, num_nulls, nulls_per_column
    except FileNotFoundError:
        print("Il file specificato non è stato trovato.")
        return -1.0, -1.0, -1.0, -1.0  # o altri valori che indicano un errore


def main():
    print(len(files))
    num_files = len(files)
    tot_rows, tot_cols, tot_nulls = 0.0, 0.0, 0.0
    avg_rows, avg_cols, avg_nulls = 0.0, 0.0, 0.0

    for file_name in files:
        file_path = os.path.join(directory, file_name)
        num_cols, num_rows, num_nulls, num_nulls_per_column = computeStats(file_path)
        tot_rows += num_rows
        tot_cols += num_cols
        tot_nulls += num_nulls  
        if num_rows != -1 and num_cols != -1 and num_nulls != -1:
            print(f"Il file {file_name} contiene {num_rows} righe e {num_cols} colonne.")
            print(f"Numero di valori nulli: {num_nulls}")
            # print(f"Numero di valori nulli per colonna: {num_nulls_per_column}\n")
        
    avg_rows = tot_rows/num_files
    avg_cols = tot_cols/num_files
    avg_nulls = tot_nulls/num_files
    # print("Numero medio di righe per file:")  
    # print(avg_rows)  
    # print("Numero medio di colonne per file:")  
    # print(avg_cols)  
    # print("Numero medio valori nulli per file:")  
    # print(avg_nulls)        


if __name__ == "__main__":
    main()

