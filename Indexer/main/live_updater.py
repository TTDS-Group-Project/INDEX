import multiprocessing
import os

import sys

from kafka import KafkaConsumer
import processer as bk
import json
import sql_interface as sq

stop_event = multiprocessing.Event()

"""
TO BE RUN LIVE
"""


def mainX():
    print("INDEXER IS ALIVE!")
    # KAFKA CONSUMER CONN DETAILS
    consumer = KafkaConsumer('articles_funnel',
                             auto_offset_reset='earliest',
                             bootstrap_servers=['35.241.142.94:9092'],
                             enable_auto_commit=True,
                             group_id='consumer_indexer',
                             sasl_mechanism="PLAIN",
                             sasl_plain_password="password",
                             sasl_plain_username="username")

    # try:

    index = bk.Inverted_Indexer()  # initialise datastructures
    atts = []

    i = 0

    while not stop_event.is_set():

        for msg in consumer:  # incoming kafka jsons
            print("NEW MESSAGE " + str(i))

            i += 1

            if stop_event.is_set():
                break

            data = json.loads(msg.value.decode('utf-8'))

            print(data['uid'])

            atts, index = bk.jproc(index, atts, data)  # doc processor

            if i % 50 == 0:  # 
                print("FLUSH: batch limit reached !!!!!!!!")
                sq.update_attributes(atts, sq.conn)
                sq.update_words(index, sq.conn)
                index = bk.Inverted_Indexer()
                atts = []
                print("DONE FLUSH")

    # final flush
    print("FLUSH: shutting down !!!!!!!!")
    sq.update_attributes(atts, sq.conn)
    sq.update_words(index, sq.conn)
    consumer.close()
    sq.conn.close()

    # except Exception as e :
    #     print(e)
    #     consumer.close()
    #     sq.conn.close()

    #     sys.exit()

    # finally:
    #     consumer.close()
    #     sq.conn.close()


if __name__ == '__main__':
    mainX()
    sys.exit()
