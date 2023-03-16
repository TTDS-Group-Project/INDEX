import json

from processer import *
import pandas as pd
# import sql_interface as sq


"""
Takes documents from files and uploads them either to sql or to correctly formatted csv files
"""


def docs_to_csv(input_file, output_path):
    index = Inverted_Indexer()
    atts = []

    df = pd.read_csv(input_file,
                     encoding='utf-8',
                     low_memory=False)

    # process docs in df to index and attributes
    for i, row in df.iterrows():
        if i % 10000 == 0:
            print(i)

        data = json.loads(row.to_json())

        atts, index = jproc(index, atts, data)

    # save files
    json_index = {}

    for key, value in index.index.items():
        json_index[key] = json.dumps(value)

    df = pd.DataFrame.from_dict(json_index, orient='index')
    df.to_csv(path_or_buf=output_path + '/windex.csv', quotechar='"', quoting=1, header=False,
              encoding='utf-8')

    df = pd.DataFrame.from_dict(atts)
    df.to_csv(path_or_buf=output_path + 'atts.csv', encoding='utf-8', na_rep='N/A',
              header=False,
              index=False)


# dataset to uploaded set db
def docs_to_db(input_file):
    index = Inverted_Indexer()
    atts = []

    df = pd.read_csv(input_file,
                     encoding='utf-8',
                     low_memory=False)

    # process docs in df to index and attributes
    for i, row in df.iterrows():
        if i % 10000 == 0:
            print(i)

        data = json.loads(row.to_json())

        atts, index = jproc(index, atts, data)

    # sq.update_attributes(atts, sq.conn)
    # sq.update_words(index, sq.conn)


def unique_keys():
    columns = ['udid',
               'title',
               'date',
               'url',
               'author',
               'publisher',
               'sentiment',
               'image',
               'category',
               'abstract']

    df = pd.read_csv('C:/Users/Euan/Documents/DATA/TTDS/FV/atts.csv', header=None,
                     names=columns)

    df = df.drop_duplicates(keep="first", subset=['udid'])
    df = df.dropna(subset=['udid'])

    df.to_csv('C:/Users/Euan/Documents/DATA/TTDS/FV/test_atts.csv', index=False, header=False, na_rep='N/A')


