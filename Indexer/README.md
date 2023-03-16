# **Indexer**

## Introduction

In main files you will find:

1. A folder named "files" containing:
   1. A kafka message format folder containing the different keys from the incoming json
   2. SQL commands: of which specicifically the commands used to make tables word_index and attributes
   3. A HELP DOC detailing more specifically some of the modules

2. PYTHON MODULES:
   1. `proccessor`: main document processor, cleaner and indexer
   2. `sql_interface`: deals with connection and sql parsing from local index and values
   3. `live_updater`: module run on the GCP service, reads from kafka -> DB
   4. `ff_updater`: module reads from csv dataset -> DB || csv index
3. TODO
4. DATA

## DATA STRUCTURES

The MAIN data structure is the INDEX. This is stored on a postgresql DB with the inverted index: <br /> 

### "_**word_index**_"

{ word: {"DOC1":[1,2,3],<br />"DOC2":[1,45,1]....},<br />
   word2: {doc->poslist},<br />
word3: {}.....

Each document ID points to an id in attributes which is used to store the documents data:

### "_**attributes**_"

udid <br />
date <br />
url <br />
title <br />
abstract <br />
author <br />
publisher <br />
image <br />
sentiment <br />
category <br />


## WORK FLOW



### Document life cycle
1. An indexer is called (FF or live)
2. The incoming docs are _pre-processed_:
   1. Ensuring certain data keys match kafka->DB 
   2. Transforming data types; date to correct format, altered uid
   3. Setting DEFAULT VALUES
   4. From a documents text (BOW creation):
      1. tokenising words
      2. removing stop words
      3. stemming
      4. case-folding
   5. abstract created from text
3. The attributes of the doc are appended to a doc attributes list
4. An inverted indexer is ran on the doc's BOW and key valued **_word_**: {"doc_id": [list,of,positions]}

### Updater life cycle

1. Set up local memory [attributes] and {index}
2. Read data
3. For docs in Data proccess (above)
4. Flush updates from data to desired location DB or file
   



#### _LIVE VS FROM FILE (FF)_ Indexing

Indexing incoming documents can either be done statically (from a single input dataset) or dynamically (from
a live source). 

A live updater will continuously read from a KAFKA pipeline and flush updates only when it reaches a certain amount of documents/memory. This is as using time as a trigger may lead to uneven updates and or not allow for live reading of documents.

A from file Updater will read from a local dataset, and update either the DB or new processed files in one go.

There are considerations to both in term of memory and run time. 
A static indexer doesn't need to consider when it should push certain docs (as all docs are processed at once and therefore flushed at once). Where as in dynamic indexing
another doc may not come in and hence the in-storage memory of the index and the attributes must be amortised at some point to avoid expensive (requiring a lot of RAM) vm for the process to run on.
