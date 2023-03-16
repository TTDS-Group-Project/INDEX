from datetime import datetime
from json import dumps

import pandas as pd
import psycopg2
from nltk.stem import *
from nltk.corpus import stopwords
import re
from dateutil.parser import *
from datetime import *


def convert_to_date(input_date):
    try:
        date_obj = datetime(1900, 1, 1)
        if type(input_date) == str:
            date_obj = parse(input_date, fuzzy=True, ignoretz=True, default=datetime(1900, 1, 1))
    except ValueError:
        date_obj = datetime(1900, 1, 1)
    return date_obj


def jproc(index, atts, data):  # process incoming json

    row = {'udid': str(data['uid']).replace("'", "").strip("b").replace("=", "").replace('"', "").replace(" ", ""),
           'title': data['headline'],
           'date': convert_to_date(data['date']).strftime("%Y-%m-%d %H:%M:%S"),
           'url': data['link'],
           'author': 'N/A',
           'publisher': data['publisher'],
           'sentiment': 'N/A',
           'image': "http://argauto.lv/application/modules/themes/views/default/assets/images/image-placeholder.png",
           'category': 'N/A',
           'abstract': 'N/A'
           }

    if 'sentiment' in data.keys():
        row['sentiment'] = data['sentiment']
    if 'topic' in data.keys():
        row['category'] = data['topic']
    if 'cover_image' in data.keys():
        row['image'] = data['cover_image']

    if 'authors' in data.keys():
        if (type(data['authors']) is list) and not (data['authors'] == []):
            row['author'] = data['authors'][0]


    row['udid'] = (row['udid'])[:255]
    # word_list = (re.split("[^a-zA-Z]", row['udid']))
    # word_list = "".join(list(filter(bool, word_list)))
    # row['udid'] = word_list

    try:
        text_ = data['text']
        abstract = create_abstract(text_)
        bow = convert_to_wordlist(text_)
        index.add_doc(row['udid'], bow)

    except:
        abstract = " "

    row['abstract'] = abstract

    atts.append(row)

    # text processing

    return atts, index


"""
Indexing
"""


class Inverted_Indexer:

    def __init__(self):

        self.index = {}

    def add_doc(self, doc_id, bow):
        doc_id = doc_id.replace("'", "").strip("b").replace("=", "")
        for position, word in enumerate(bow):
            word = word[:255]
            if word not in self.index:
                self.index[word] = {}
            if doc_id not in self.index[word]:
                self.index[word][doc_id] = []
            if position not in self.index[word][doc_id]:
                self.index[word][doc_id].append(position)

    """
    end functions from Indexer
    """


def windex_to_csv(index):
    with open('../../../data/dataset/windex.csv', 'w', encoding='utf-8') as f:
        for word, json in index.index.items():
            j = json_to_sql(json)

            line = word + "," + j

            f.write(line)


def json_to_sql(json):
    json = dumps(json)
    json = '"' + json.replace('"', '""') + '"\n'

    return json


def create_abstract(text):
    shortString = text[:200] + ('...' if len(text) > 197 else '')
    return shortString


"""

pre-processing functions

"""

stopWords = set(stopwords.words('english'))
stem = PorterStemmer()


def tokenise(words):
    word_list = (re.split("[^a-zA-Z]", words))
    word_list = list(filter(bool, word_list))

    return word_list


def case_fold(word_list):
    word_list = [i.lower() for i in word_list]
    word_list = [i[:254] for i in word_list]

    return word_list


def sw_remove(word_list):
    for i in stopWords:
        word_list = [j for j in word_list if j != i]

    return word_list


def stemmer(word_list):
    word_list = [stem.stem(i) for i in word_list]

    return word_list


def convert_to_wordlist(word_list):
    word_list = tokenise(word_list)
    word_list = case_fold(word_list)
    word_list = sw_remove(word_list)
    word_list = stemmer(word_list)

    return word_list


def list_to_string(list_):
    out = ""
    i = 0
    while i < len(list_) - 1:
        out += str(list_[i])
        out += ","
        i += 1
    out += str(list_[-1])

    return out


def index_toText(index, f):
    for word in index:

        f.write(word + ":\n")
        word_dic = index.get(word)

        if type(word_dic) is int:
            f.write("\t" + str(word_dic) + "\n")
            continue

        for document in word_dic:
            f.write("\t" + document + "\n")

        f.write("\n")


"""
end pre-processing functions
"""
