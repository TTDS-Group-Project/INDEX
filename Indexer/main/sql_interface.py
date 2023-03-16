import csv
import json
import os
import psycopg2

HOST = '/cloudsql/{}'.format("ttds-group-project-376615:europe-west1:v1")

conn = None
if os.getenv("K_REVISION"):
    conn = psycopg2.connect(
    dbname="v6-FV",
    user="postgres",
    password="ttds1234",
    host=HOST,
    )
else:
    conn = psycopg2.connect(
        dbname="v6-FV",
        user="postgres",
        password="ttds1234",
        host="34.76.187.212",
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

    # print("ATTRIBUTES !!!!!!!!!!!!!!!!!!!!!! ")
    # print(atts)

    # Execute insert statement
    cur.executemany(insert_statement, atts)

    # sql connection commit and close
    conn.commit()


def update_words(index, conn):
    cur = conn.cursor()

    for word, value in index.index.items():
        # print("KEY: " + word)
        # print("VALUE: " + str(value))

        value = json.dumps(value)

        wi = value.replace('\\', '').replace('""', '"') # 

        execute_stmt = create_upsert(word, wi)  # returns update string with word and json formatted
        # print("STATEMENT " +  execute_stmt)

        cur.execute(execute_stmt)
        conn.commit()

    # commit and close
    conn.commit()


def create_upsert(word, index):
    upsert_statement = f"""INSERT INTO word_index (word, index) 
    VALUES ('{word}','{index}'::jsonb) 
    ON CONFLICT (word) DO UPDATE SET index = word_index.index || excluded.index;"""

    return upsert_statement
