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
    df = {'name': ['Lobra', 'Lobra','Lobbi','Lobbi'], 
        'country': ['United Kindom', 'UK','UK','UK'],
        'employees': [None, None,'300',None],
        'founded': [None, '1900',None,'1800']}

    #data = pd.DataFrame(df)

    # Riempimento dei valori mancanti in df1 con quelli di df2
    recordLinkageObj = RecordLinkageClass()
   # recordLinkageObj.recordLinkageMethod(data)

    # with open('sample.json', 'r') as file:
    #     data = json.load(file)

    data = pd.read_csv('schema_matching_file.csv',encoding='latin1')
    print("COLONNE")
    print(data.columns.tolist())
    blockList = []
    for key in data.keys():
        if len(data[key]) > 1 :
            dfBlock = pd.DataFrame(data[key])
            country_names = coco.convert(names=dfBlock['country'], to='ISO3')
            country_names = list(map(lambda x: x.replace('not found', ''), country_names))
            dfBlock['country'] = country_names
            blockList.append(recordLinkageObj.recordLinkageMethod(dfBlock))

    final_df = pd.concat(blockList, ignore_index=True)
    final_df.to_csv("record_linkage.csv", index=False)

def transformString(name):
    nameLower = name.lower()
    result = ''.join(e for e in nameLower if e.isalnum())
    return result

def process_chunk(chunk):
    chunk.fillna('', inplace=True)
    name_dict = {}
    for _, row in chunk.iterrows():
        name = row['name']
        nameChanged = transformString(name)
        row_dict = row.to_dict()
        if nameChanged[:2] in name_dict:
            name_dict[nameChanged[:2]].append(row_dict)
        else:
            name_dict[nameChanged[:2]] = [row_dict]
    return name_dict

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
    return_dict = manager.dict()

    chunks = pd.read_csv(filename, chunksize=1000, encoding='latin-1')
    results = pool.map(process_chunk, chunks)

    for result in results:
        return_dict = merge_dicts(return_dict, result)

    pool.close()
    pool.join()

    return dict(return_dict)
def main():
    filename = 'schema_matching_file.csv'
    matches_dict = {'industry': [('cbinsights.csv', 'industry'), ('AmbitionBox.csv', 'Industry'), ('DDD-teamblind.com.csv', 'industry'), ('DDD-cbinsight.com.csv', 'industry'), ('ft.com.csv', 'industry'), ('gren-disfold.com.csv', 'industry'), ('output_globaldata.csv', 'industry'), ('output_govuk_bigsize.csv', 'nature_of_business'), ('output_wiki.csv', 'industry'), ('wiki.csv', 'Industry')], 'country': [('companiesmarketcap.csv', 'country'), ('cbinsights.csv', 'country'), ('ft.com.csv', 'country'), ('DDD-cbinsight.com.csv', 'country'), ('ft.csv', 'Country'), ('gren-disfold.com.csv', 'country'), ('gren-companiesmarketcap.com.csv', 'country'), ('famcap_germany.csv', 'State (abbreviation)'), ('gren-ft.com.csv', 'country'), ('silvestri-forbes.com.csv', 'country'), ('valuetoday.csv', 'Country'), ('valueToday2.csv', 'country')], 'name': [('companiesmarketcap.csv', 'name'), ('AmbitionBox.csv', 'Name'), ('DDD-teamblind.com.csv', 'name'), ('DDD-cbinsight.com.csv', 'name'), ('ft.com.csv', 'name'), ('disfold.com.csv', 'name'), ('ft.csv', 'Name'), ('gren-disfold.com.csv', 'name'), ('gren-companiesmarketcap.com.csv', 'name'), ('gren-ft.com.csv', 'name'), ('output_globaldata.csv', 'name'), ('output_govuk_bigsize.csv', 'name'), ('output_wiki.csv', 'name'), ('silvestri-forbes.com.csv', 'name'), ('valuetoday.csv', 'Name'), ('valueToday2.csv', 'name'), ('wiki.csv', 'Name'), ('wissel-aziende-gov.uk.csv', 'Name')], 'datejoined': [('DDD-cbinsight.com.csv', 'dateJoined'), ('cbinsights.csv', 'datejoined')], 'city': [('DDD-cbinsight.com.csv', 'city'), ('cbinsights.csv', 'city'), ('DDD-teamblind.com.csv', 'locations')], 'rank': [('DDD-cbinsight.com.csv', 'rank'), ('companiesmarketcap.csv', 'rank'), ('famcap_germany.csv', 'Rank'), ('DDD-teamblind.com.csv', 'rank'), ('ft.csv', 'Rank'), ('ft.csv', 'in 2021 ranking'), ('ft.csv', 'in 2020 ranking'), ('valueToday2.csv', 'world_rank')], 'valuation': [('DDD-cbinsight.com.csv', 'valuation'), ('cbinsights.csv', 'valuation')], 'investors': [('DDD-cbinsight.com.csv', 'investors'), ('cbinsights.csv', 'selectinvestors')], 'founded': [('DDD-cbinsight.com.csv', 'founded'), ('AmbitionBox.csv', 'Foundation Year'), ('famcap_germany.csv', 'Founded'), ('DDD-teamblind.com.csv', 'founded'), ('ft.csv', 'Founding Year'), ('ft.com.csv', 'founded'), ('gren-ft.com.csv', 'founding_year'), ('output_govuk_bigsize.csv', 'company_creation_date'), ('output_wiki.csv', 'founded'), ('valuetoday.csv', 'Founded Year'), ('wiki.csv', 'Founded')], 'headquarter': [('disfold.com.csv', 'headquarters'), ('AmbitionBox.csv', 'Headquarter'), ('famcap_germany.csv', 'Headquarters'), ('output_globaldata.csv', 'headquarters'), ('output_wiki.csv', 'headquarters'), ('wiki.csv', 'Headquarters')], 'marketcap': [('disfold.com.csv', 'market_cap'), ('companiesmarketcap.csv', 'marketcap'), ('gren-disfold.com.csv', 'market_capitalization_USD'), ('gren-companiesmarketcap.com.csv', 'market_capitalization_USD'), ('output_globaldata.csv', 'market_cap'), ('valueToday2.csv', 'marketCap')], 'employees': [('disfold.com.csv', 'employees'), ('DDD-teamblind.com.csv', 'size'), ('ft.com.csv', 'employees'), ('famcap_germany.csv', 'Employees'), ('ft.csv', 'Number of employees 2020'), ('ft.csv', 'Number of employees 2017'), ('gren-ft.com.csv', 'employees_number_2020'), ('gren-ft.com.csv', 'employees_number_2017'), ('output_globaldata.csv', 'number_of_employees'), ('output_wiki.csv', 'number_of_employees'), ('valuetoday.csv', 'Number of Employees')], 'company': [('famcap_germany.csv', 'Company'), ('cbinsights.csv', 'company')], 'website': [('famcap_germany.csv', 'Website'), ('DDD-teamblind.com.csv', 'website'), ('output_globaldata.csv', 'website'), ('output_wiki.csv', 'website')], 'ownership': [('famcap_germany.csv', 'Family ownership'), ('AmbitionBox.csv', 'Ownership')], 'link': [('ft.com.csv', 'link'), ('disfold.com.csv', 'link')], 'revenue': [('ft.com.csv', 'revenue'), ('famcap_germany.csv', 'Revenues 2018 $m'), ('ft.csv', 'Revenue 2020 (euro)'), ('ft.csv', 'Revenue 2017 (euro)'), ('gren-ft.com.csv', 'revenue_2020_EU'), ('gren-ft.com.csv', 'revenue_2017_EU'), ('output_globaldata.csv', 'revenue'), ('output_wiki.csv', 'revenue'), ('silvestri-forbes.com.csv', 'revenue_2022')], 'sector': [('ft.csv', 'Sector'), ('famcap_germany.csv', 'Sector'), ('gren-ft.com.csv', 'sector'), ('gren-disfold.com.csv', 'sector'), ('wiki.csv', 'Sector')], 'address': [('output_govuk_bigsize.csv', 'registered_office_address'), ('output_globaldata.csv', 'address'), ('wissel-aziende-gov.uk.csv', 'Office Address')], 'type': [('output_wiki.csv', 'type'), ('output_govuk_bigsize.csv', 'company_type'), ('famcap_germany.csv', 'Public/Private'), ('wissel-aziende-gov.uk.csv', 'Company Type')], 'ceo': [('valuetoday.csv', 'CEO'), ('disfold.com.csv', 'ceo'), ('valueToday2.csv', 'CEO')], 'market_value': [('valuetoday.csv', 'Market Value'), ('silvestri-forbes.com.csv', 'market_value_apr_2022'), ('valueToday2.csv', 'marketValue')], 'company_status': [('wissel-aziende-gov.uk.csv', 'Company Status'), ('output_govuk_bigsize.csv', 'company_status')], 'company_number': [('wissel-aziende-gov.uk.csv', 'Company ID'), ('output_govuk_bigsize.csv', 'company_number')]}
    matches_list = matches_dict.keys()
    matchingClass = SchemaMatching()
    matchingClass.makeSchemaMatching(matches_list, matches_dict)
    df = pd.read_csv(filename, encoding='latin-1')
    print(df.isnull().sum())
    name_dict = create_dictionary(filename)
    # for name, rows in name_dict.items():
    #     print(f"Name: {name}, Rows: {rows}")

    print(len(name_dict.keys()))
    with open("sample.json", "w") as outfile: 
        json.dump(name_dict, outfile, indent=4)

    # recordLinkageFromBlocking()

if __name__ == "__main__":
    main()
