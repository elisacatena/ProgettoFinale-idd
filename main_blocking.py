import json
import pandas as pd
import multiprocessing as mp
import numpy as np
from record_linkage import RecordLinkageClass
import country_converter as coco
from schema_matching import SchemaMatching


def recordLinkageFromBlocking():

    # Creazione dei DataFrame
    # df1 = pd.DataFrame({'name': ['ciao'], 'A': [None], 'B': ['b']})
    # df2 = pd.DataFrame({'name': ['ciao'], 'A': ['a'], 'B': [None]})
    # df = {'name': ['Lobra', 'Lobra','Lobra','Lobra','Apple','Apple'], 
    #       'country': ['UK', 'UK','italy','UK','USA','USA'],
    #       'employess': [None, None,'300','100','500','600'],
    #       'founded': [None, '1900',None, None,None,'2010']}
    # df = {'name': ['Lobra', 'Lobra','Lobbi','Lobra', 'Lobbi'], 
    #     'industry': ['tech', 'tech', 'ia', 'health', 'ia'],
    #     'employees': ['60', None,'300', '20', '40'],
    #     'founded': [None, '1900',None,'1800', '2000']}

    # data = pd.DataFrame(df)

    # Riempimento dei valori mancanti in df1 con quelli di df2
    recordLinkageObj = RecordLinkageClass()

    with open('blockingByCountry.json', 'r') as file:
    # with open('prova.json', 'r') as file:
        data = json.load(file)

    blockList = []
    for key in data:
        if len(data[key]) > 1 :
            dfBlock = pd.DataFrame(data[key])
            # country_names = coco.convert(names=dfBlock['country'], to='ISO3')
            # country_names = list(map(lambda x: x.replace('not found', ''), country_names))
            # dfBlock['country'] = country_names
            blockList.append(recordLinkageObj.recordLinkageMethod(dfBlock))
    # block = recordLinkageObj.recordLinkageMethod(data)
    # blockList.append(block)
    final_df = pd.concat(blockList, ignore_index=True)
    final_df.to_csv("record_linkage_country.csv", index=False)
    # final_df.to_csv("prova.csv", index=False)

def transformString(name):
    nameLower = name.lower()
    result = ''.join(e for e in nameLower if e.isalnum())
    return result

def process_chunk(chunk):
    chunk.fillna('', inplace=True)
    name_dict = {}
    country_dict = {}
    for _, row in chunk.iterrows():
        name = row['name']
        country = row['country']
        nameChanged = transformString(name)
        countryChanged = transformString(country)
        row_dict = row.to_dict()
        if nameChanged[:2] in name_dict:
            name_dict[nameChanged[:2]].append(row_dict)
        else:
            name_dict[nameChanged[:2]] = [row_dict]
        if countryChanged[:2] in country_dict:
            country_dict[countryChanged[:2]].append(row_dict)
        else:
            country_dict[countryChanged[:2]] = [row_dict]
    return (name_dict, country_dict)

def merge_dicts(dict1, dict2):
    for key, value in dict2.items():
        if key in dict1:
            dict1[key] += value
        else:
            dict1[key] = value
    return dict1

def create_dictionary(filename):
    pool_size = mp.cpu_count()  # Imposta il numero di processi da utilizzare
    pool = mp.Pool(pool_size)
    manager = mp.Manager()
    return_dict_name = manager.dict()
    return_dict_country = manager.dict()


    chunks = pd.read_csv(filename, chunksize=1000, encoding='latin-1')
    results = pool.map(process_chunk, chunks)

    for result in results:
        return_dict_name = merge_dicts(return_dict_name, result[0])
        return_dict_country = merge_dicts(return_dict_country, result[1])

    pool.close()
    pool.join()

    return (dict(return_dict_name), dict(return_dict_country))

def checkNullAttributes():
    import pandas as pd
    
    # Carica il file CSV
    df = pd.read_csv('schema_matching_file.csv', encoding='latin-1')
    
    # Controlla se 'country' è nullo ma 'address' non lo è
    condizione_1 = df['country'].isnull() & ~df['address'].isnull()
    row_country_null_address_not_null = df[condizione_1]

    # Controlla se 'country' e 'address' sono entrambi nulli ma 'city' non lo è
    condizione_2 = df['country'].isnull() & df['address'].isnull() & ~df['city'].isnull()
    row_country_null_city_not_null = df[condizione_2]

    # Correggi 'country' per le righe con 'country' nullo ma 'address' non nullo
    for index, row in row_country_null_address_not_null.iterrows():
        address = row['address'].split(',')
        df.at[index, 'country'] = address[len(address)-2]

    # Correggi 'country' per le righe con 'country' e 'address' entrambi nulli ma 'city' non nullo
    for index, row in row_country_null_city_not_null.iterrows():
        city = row['city'].split(',')
        df.at[index, 'country'] = city[len(city)-1]
    
    # Salva il DataFrame modificato nel file CSV
    df.to_csv('country_schema_matching_file.csv', encoding='latin-1', index=False)

    print(df.isnull().sum())

    condizione_3 = df['country'].isnull() & ~df['headquarter'].isnull()
    row_country_null_headquarter_not_null = df[condizione_3]

    for index, row in row_country_null_headquarter_not_null.iterrows():
        headquarter = row['headquarter'].split(',')
        df.at[index, 'country'] = headquarter[len(headquarter)-1]

    df.to_csv('country_schema_matching_file.csv', encoding='latin-1', index=False)

    print(df.isnull().sum())

def main():
    # filename = 'country_schema_matching_file.csv'
    # # matches_dict = {'industry': [('cbinsights.csv', 'industry'), ('AmbitionBox.csv', 'Industry'), ('DDD-teamblind.com.csv', 'industry'), ('DDD-cbinsight.com.csv', 'industry'), ('ft.com.csv', 'industry'), ('gren-disfold.com.csv', 'industry'), ('output_globaldata.csv', 'industry'), ('output_govuk_bigsize.csv', 'nature_of_business'), ('output_wiki.csv', 'industry'), ('wiki.csv', 'Industry')], 'country': [('companiesmarketcap.csv', 'country'), ('cbinsights.csv', 'country'), ('ft.com.csv', 'country'), ('DDD-cbinsight.com.csv', 'country'), ('ft.csv', 'Country'), ('gren-disfold.com.csv', 'country'), ('gren-companiesmarketcap.com.csv', 'country'), ('famcap_germany.csv', 'State (abbreviation)'), ('gren-ft.com.csv', 'country'), ('silvestri-forbes.com.csv', 'country'), ('valuetoday.csv', 'Country'), ('valueToday2.csv', 'country')], 'name': [('companiesmarketcap.csv', 'name'), ('AmbitionBox.csv', 'Name'), ('DDD-teamblind.com.csv', 'name'), ('DDD-cbinsight.com.csv', 'name'), ('ft.com.csv', 'name'), ('disfold.com.csv', 'name'), ('ft.csv', 'Name'), ('gren-disfold.com.csv', 'name'), ('gren-companiesmarketcap.com.csv', 'name'), ('gren-ft.com.csv', 'name'), ('output_globaldata.csv', 'name'), ('output_govuk_bigsize.csv', 'name'), ('output_wiki.csv', 'name'), ('silvestri-forbes.com.csv', 'name'), ('valuetoday.csv', 'Name'), ('valueToday2.csv', 'name'), ('wiki.csv', 'Name'), ('wissel-aziende-gov.uk.csv', 'Name')], 'datejoined': [('DDD-cbinsight.com.csv', 'dateJoined'), ('cbinsights.csv', 'datejoined')], 'city': [('DDD-cbinsight.com.csv', 'city'), ('cbinsights.csv', 'city'), ('DDD-teamblind.com.csv', 'locations')], 'rank': [('DDD-cbinsight.com.csv', 'rank'), ('companiesmarketcap.csv', 'rank'), ('famcap_germany.csv', 'Rank'), ('DDD-teamblind.com.csv', 'rank'), ('ft.csv', 'Rank'), ('ft.csv', 'in 2021 ranking'), ('ft.csv', 'in 2020 ranking'), ('valueToday2.csv', 'world_rank')], 'valuation': [('DDD-cbinsight.com.csv', 'valuation'), ('cbinsights.csv', 'valuation')], 'investors': [('DDD-cbinsight.com.csv', 'investors'), ('cbinsights.csv', 'selectinvestors')], 'founded': [('DDD-cbinsight.com.csv', 'founded'), ('AmbitionBox.csv', 'Foundation Year'), ('famcap_germany.csv', 'Founded'), ('DDD-teamblind.com.csv', 'founded'), ('ft.csv', 'Founding Year'), ('ft.com.csv', 'founded'), ('gren-ft.com.csv', 'founding_year'), ('output_govuk_bigsize.csv', 'company_creation_date'), ('output_wiki.csv', 'founded'), ('valuetoday.csv', 'Founded Year'), ('wiki.csv', 'Founded')], 'headquarter': [('disfold.com.csv', 'headquarters'), ('AmbitionBox.csv', 'Headquarter'), ('famcap_germany.csv', 'Headquarters'), ('output_globaldata.csv', 'headquarters'), ('output_wiki.csv', 'headquarters'), ('wiki.csv', 'Headquarters')], 'marketcap': [('disfold.com.csv', 'market_cap'), ('companiesmarketcap.csv', 'marketcap'), ('gren-disfold.com.csv', 'market_capitalization_USD'), ('gren-companiesmarketcap.com.csv', 'market_capitalization_USD'), ('output_globaldata.csv', 'market_cap'), ('valueToday2.csv', 'marketCap')], 'employees': [('disfold.com.csv', 'employees'), ('DDD-teamblind.com.csv', 'size'), ('ft.com.csv', 'employees'), ('famcap_germany.csv', 'Employees'), ('ft.csv', 'Number of employees 2020'), ('ft.csv', 'Number of employees 2017'), ('gren-ft.com.csv', 'employees_number_2020'), ('gren-ft.com.csv', 'employees_number_2017'), ('output_globaldata.csv', 'number_of_employees'), ('output_wiki.csv', 'number_of_employees'), ('valuetoday.csv', 'Number of Employees')], 'company': [('famcap_germany.csv', 'Company'), ('cbinsights.csv', 'company')], 'website': [('famcap_germany.csv', 'Website'), ('DDD-teamblind.com.csv', 'website'), ('output_globaldata.csv', 'website'), ('output_wiki.csv', 'website')], 'ownership': [('famcap_germany.csv', 'Family ownership'), ('AmbitionBox.csv', 'Ownership')], 'link': [('ft.com.csv', 'link'), ('disfold.com.csv', 'link')], 'revenue': [('ft.com.csv', 'revenue'), ('famcap_germany.csv', 'Revenues 2018 $m'), ('ft.csv', 'Revenue 2020 (euro)'), ('ft.csv', 'Revenue 2017 (euro)'), ('gren-ft.com.csv', 'revenue_2020_EU'), ('gren-ft.com.csv', 'revenue_2017_EU'), ('output_globaldata.csv', 'revenue'), ('output_wiki.csv', 'revenue'), ('silvestri-forbes.com.csv', 'revenue_2022')], 'sector': [('ft.csv', 'Sector'), ('famcap_germany.csv', 'Sector'), ('gren-ft.com.csv', 'sector'), ('gren-disfold.com.csv', 'sector'), ('wiki.csv', 'Sector')], 'address': [('output_govuk_bigsize.csv', 'registered_office_address'), ('output_globaldata.csv', 'address'), ('wissel-aziende-gov.uk.csv', 'Office Address')], 'type': [('output_wiki.csv', 'type'), ('output_govuk_bigsize.csv', 'company_type'), ('famcap_germany.csv', 'Public/Private'), ('wissel-aziende-gov.uk.csv', 'Company Type')], 'ceo': [('valuetoday.csv', 'CEO'), ('disfold.com.csv', 'ceo'), ('valueToday2.csv', 'CEO')], 'market_value': [('valuetoday.csv', 'Market Value'), ('silvestri-forbes.com.csv', 'market_value_apr_2022'), ('valueToday2.csv', 'marketValue')], 'company_status': [('wissel-aziende-gov.uk.csv', 'Company Status'), ('output_govuk_bigsize.csv', 'company_status')], 'company_number': [('wissel-aziende-gov.uk.csv', 'Company ID'), ('output_govuk_bigsize.csv', 'company_number')]}
    # # matches_list = matches_dict.keys()
    # # matchingClass = SchemaMatching()
    # # matchingClass.makeSchemaMatching(matches_list, matches_dict)
    # # df = pd.read_csv(filename, encoding='latin-1')

    # checkNullAttributes()

    # # print(df.isnull().sum())
    # couple_dict = create_dictionary(filename)

    # # print(len(name_dict.keys()))
    # with open("blockingByName.json", "w") as outfile: 
    #     json.dump(couple_dict[0], outfile, indent=4)

    # with open("blockingByCountry.json", "w") as outfile: 
    #     json.dump(couple_dict[1], outfile, indent=4)

    recordLinkageFromBlocking()

if __name__ == "__main__":
    main()
