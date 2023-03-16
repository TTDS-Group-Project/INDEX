import csv
import json

import psycopg2

unix_socket = '/cloudsql/{}'.format("ttds-group-project-376615:europe-west1:v1")

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname="v6-FV",
    user="postgres",
    password="TTDS1234",
    host=unix_socket,
    port="5432"
)



def update_attributes(atts_list, conn):
    cur = conn.cursor()

    # SQL insert statement
    insert_statement = "INSERT INTO attributes " \
                       "(udid, title, date, url, author, publisher, sentiment, image, category, abstract)" \
                       " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
                       "ON CONFLICT (udid) DO NOTHING;"

    # Take values from atts
    atts = [(row['udid'],
             row['title'],
             row['date'],
             row['url'],
             row['author'],
             row['publisher'],
             row['sentiment'],
             row['image'],
             row['category'],
             row['abstract'])

            for row in atts_list]

    # Execute insert statement
    cur.executemany(insert_statement, atts)

    # sql connection commit and close
    conn.commit()


def update_words(index, conn):
    cur = conn.cursor()

    for key, value in index.index.items():

        value = json.dumps(value)

        word = key
        wi = value.replace('\\', '').replace('""', '"') # 

        execute_stmt = create_upsert(word, wi)  # returns update string with word and json formatted

        cur.execute(execute_stmt)
        conn.commit()

    # commit and close
    conn.commit()


def create_upsert(word, index):
    upsert_statement = f"""INSERT INTO word_index (word, index) 
    VALUES ('{word}','{index}'::jsonb) 
    ON CONFLICT (word) DO UPDATE SET index = word_index.index || excluded.index;"""

    return upsert_statement
