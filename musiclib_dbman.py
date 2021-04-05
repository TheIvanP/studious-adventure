#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages

# In[1]:


# Import Python packages
import re
import os
import glob
import json
import csv

import numpy as np
import pandas as pd
import cassandra


# #### Creating list of filepaths to process original event csv data files

# In[2]:
# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + "/event_data"

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):

    # join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root, "*"))
    # print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[3]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = []

# for every filepath in the file path list
for f in file_path_list:

    # reading csv file
    with open(f, "r", encoding="utf8", newline="") as csvfile:
        # creating a csv reader object
        csvreader = csv.reader(csvfile)
        next(csvreader)

        # extracting each data row one by one and append it
        for line in csvreader:
            # print(line)
            full_data_rows_list.append(line)

# uncomment the code below if you would like to get total number of rows
# print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
# print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect("myDialect", quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open("event_datafile_new.csv", "w", encoding="utf8", newline="") as f:
    writer = csv.writer(f, dialect="myDialect")
    writer.writerow(
        [
            "artist",
            "firstName",
            "gender",
            "itemInSession",
            "lastName",
            "length",
            "level",
            "location",
            "sessionId",
            "song",
            "userId",
        ]
    )
    for row in full_data_rows_list:
        if row[0] == "":
            continue
        writer.writerow(
            (
                row[0],
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[12],
                row[13],
                row[16],
            )
        )


# In[4]:


# check the number of rows in your csv file
with open("event_datafile_new.csv", "r", encoding="utf8") as f:
    print(sum(1 for line in f))


# # Part II. Complete the Apache Cassandra coding portion of your project.
#
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns:
# - artist
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
#
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
#
# <img src="images/image_event_datafile_new.jpg">

# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[ ]:


# This should make a connection to a Cassandra instance your local machine
# (127.0.0.1)

from cassandra.cluster import Cluster

cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace

# In[ ]:

try:
    session.execute(
        """
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
    )

except Exception as e:
    print(e)


# #### Set Keyspace

# In[ ]:
try:
    session.set_keyspace("udacity")
except Exception as e:
    print(e)

#%%
# ### Drop table before creating if if it exists
tables = ("music_library_q1", "music_library_q2", "music_library_q3")


def drop_table(tables: tuple) -> None:
    for table in tables:
        query = f"DROP TABLE IF EXISTS {table}"
        try:
            session.execute(query)
        except Exception as e:
            print(e)


# process tables
drop_table(tables)
# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.
#%%

#%%

# ## Create queries to ask the following three questions of the data
#
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
query = "CREATE TABLE IF NOT EXISTS music_library_q1 "
query = (
    query
    + """(artist text, song text, item_in_session int, length float, session_id int,  
                PRIMARY KEY (session_id, item_in_session))"""
)
try:
    session.execute(query)
except Exception as e:
    print(e)
#%%
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

query = "CREATE TABLE IF NOT EXISTS music_library_q2 "
query = (
    query
    + """(artist text, song text, first_name text, last_name text, item_in_session int, length float, level text, location text, session_id int, user_id int, 
                PRIMARY KEY (user_id, session_id, item_in_session))"""
)
try:
    session.execute(query)
except Exception as e:
    print(e)

#%%

# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

query = """CREATE TABLE IF NOT EXISTS music_library_q3 """
query = (
    query
    + """(song text, first_name text, last_name text, 
                PRIMARY KEY (song, first_name, last_name))"""
)
try:
    session.execute(query)
except Exception as e:
    print(e)
#%%
# map columns to order they appear in csv file line
# columns from csv in order
# 0 artist
# 1 firstName
# 2 gender
# 3 itemInSession
# 4 lastName
# 5 length
# 6 level
# 7 location
# 8 sessionId
# 9 song
# 10 userId
col_map = {
    "artist": 0,
    "first_name": 1,
    "gender": 2,
    "item_in_session": 3,
    "last_name": 4,
    "length": 5,
    "level": 6,
    "location": 7,
    "session_id": 8,
    "song": 9,
    "user_id": 10,
}

# In[1]:


## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4


# In[ ]:
#%%
import pandas as pd

song_csv = pd.read_csv("event_datafile_new.csv")


# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = "event_datafile_new.csv"

columns_to_insert = [
    "artist",
    "song",
    "item_in_session",
    "length",
    "session_id",
]

col_name_file_map = {
    "artist": "artist",
    "first_name": "firstName",
    "gender": "gender",
    "item_in_session": "itemInSession",
    "last_name": "lastName",
    "length": "length",
    "level": "level",
    "location": "location",
    "session_id": "sessionId",
    "song": "song",
    "user_id": "userId",
}


# read with fixed chunksize = fixed memory usage
df_songdata_chunks = pd.read_csv("event_datafile_new.csv", chunksize=100000)


def get_value_from_csv(row, key: str):
    return row._asdict().get(col_name_file_map.get(key))


def insert_music_library_col(row, query_cols: tuple, table_name: str, session) -> None:
    """Construct query to insert row of data from pandas itetuple object using
    list of columns into the named table and execute it with the
    given cassandra session

    Args:
            row:pandas object   row from pandas itertuple method
            query_cols:tuple    list of column names to insert
            table_name:str      name of the table to insert into
            session             cassandra session object

    Returns:
            None

    """
    #  get elements in query_cols as one string for insertion into query
    query_cols_asstring = ", ".join(query_cols)

    #  compose query insert statement
    query_insert = f"INSERT INTO {table_name} ({query_cols_asstring}) "

    #  for each element in query cols create value substitution magics
    subs = ", ".join(["%s" for x in query_cols])

    #  compose value insertion query string
    values = f"VALUES ({subs})"

    #  get the data from row looking up the column names
    cols = [get_value_from_csv(row, col_name) for col_name in query_cols]
    #        try:

    # execute the session, casting cols to tuple
    session.execute(
        f"{query_insert}{values}",
        tuple(cols),
    )


#  ---------- DATA FOR QUERY CONSTRUCTION ---------------
# Query 1 args:
q1_cols = ("artist", "song", "item_in_session", "length", "session_id")
q1_table_name = "music_library_q1"

# Query 2 args:
q2_cols = (
    "artist",
    "song",
    "first_name",
    "last_name",
    "item_in_session",
    "length",
    "level",
    "location",
    "session_id",
    "user_id",
)
q2_table_name = "music_library_q2"

# Query 3 args:
q3_cols = ("song", "first_name", "last_name")
q3_table_name = "music_library_q3"

#%%
for chunk in df_songdata_chunks:
    for row in chunk.itertuples(index=False):
        # Query 1"
        insert_music_library_col(row, q1_cols, q1_table_name, session)

        # Query 2"
        insert_music_library_col(row, q2_cols, q2_table_name, session)

        # Query 3"
        insert_music_library_col(row, q3_cols, q3_table_name, session)


#%%
# Test query 1
query = "SELECT * FROM music_library_q1 WHERE session_id = 338 AND item_in_session = 4 "
# try:
rows = session.execute(query)
# except Exception as e:
#    print(e)
for row in rows:
    #    print(row.artist, row.song, row.item_in_session, row.length, row.session_id)
    print(row)

#%%
# Test query 2
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
query = "SELECT * FROM music_library_q2 WHERE user_id = 10 AND session_id = 182 ORDER BY item_in_session "
# try:
rows = session.execute(query)
# except Exception as e:
#    print(e)

for row in rows:
    [pp.pprint([k, v]) for k, v in row._asdict().items()]


# Test query 3...
#%%
import pprint

pp = pprint.PrettyPrinter(indent=4)


#%%
# Test query 3
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
query = "SELECT first_name, last_name FROM music_library_q3 where song = 'All Hands Against His Own'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
for row in rows:
    rowdict []
    
    for column_name, value in row._asdict().items():

        pp.pprint([column_name, value])

#%%
with open(file, encoding="utf8") as f:
    csvreader = csv.reader(f)
    next(csvreader)  # skip header

    for line in csvreader:
        query = "INSERT INTO music_library_q1 (artist, song, item_in_session, length, session_id) "
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        cols = ["artist", "song", "item_in_session", "length", "session_id"]

        #        try:
        session.execute(
            query,
            tuple([line[col_map.get(col)] for col in cols]),
        )
#        except Exception as e:
#            print(e)
## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`


# #### Do a SELECT to verify that the data have been inserted into each table

# In[ ]:

## TO-DO: Add in the SELECT statement to verify the data was entered into the table
#%%

# ### COPY AND REPEAT THE ABOVE THREE CELLS FOR EACH OF THE THREE QUESTIONS

# In[ ]:


## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182


# In[ ]:


## TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'


# In[ ]:


# In[ ]:


# ### Drop the tables before closing out the sessions

# In[4]:


## TO-DO: Drop the table before closing out the sessions


# In[ ]:


# ### Close the session and cluster connection¶

# In[ ]:


# session.shutdown()
# cluster.shutdown()


#%%
