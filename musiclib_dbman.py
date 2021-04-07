import pandas as pd
from cassandra.cluster import Cluster

from helpers import (
    construct_create_table_query,
    dict_to_insert_string,
    drop_table,
    insert_music_library_col,
    process_files,
    test_query,
    create_table,
)

# ------------- globals ---------------------
FILE = "event_datafile_new.csv"


# ----------- process csv files ----------------------------------
process_files()

# ----------- set up cassandra  connection and keyspace --------------

# instantiate cassandra cluster object
cluster = Cluster()

# create a session. assuming localhost connection
try:
    session = cluster.connect()
except Exception as e:
    print(e)

# #### Create Keyspace
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
try:
    session.set_keyspace("udacity")
except Exception as e:
    print(e)

#%%
# ----------- create new tables based on queries ---------------------


#  ---------- Query 1 -------------------
query_statement_1 = """Give me the artist, song title and song's length in the music app history
                    that was heard during  sessionId = 338, and itemInSession  = 4"""
table_q1 = "music_library_q1"
q1_cols = {
    "artist": "text",
    "song": "text",
    "item_in_session": "int",
    "length": "float",
    "session_id": "int",
}
q1_primary_key = "session_id, item_in_session"

q1_create_table_query = construct_create_table_query(
    dict_to_insert_string(q1_cols), primary_key=q1_primary_key
)

#  ---------- Query 2 -------------------
query_statement_2 = """Give me only the following: name of artist, song (sorted by itemInSession) 
                        and user (first and last name) for userid = 10, sessionid = 182"""
# Query 2 args:
table_q2 = "music_library_q2"
q2_cols = {
    "artist": "text",
    "song": "text",
    "first_name": "text",
    "last_name": "text",
    "item_in_session": "int",
    "length": "float",
    "level": "text",
    "location": "text",
    "session_id": "int",
    "user_id": "int",
}
q2_primary_key = "user_id, session_id, item_in_session"

q2_create_table_query = construct_create_table_query(
    dict_to_insert_string(q2_cols), primary_key=q2_primary_key
)


#  ---------- Query 3 -------------------
query_statement_3 = """Give me every user name (first and last) in my music app history
                    who listened to the song 'All Hands Against His Own"""

table_q3 = "music_library_q3"
q3_cols = {
    "song": "text",
    "first_name": "text",
    "last_name": "text",
}
q3_primary_key = "song, first_name, last_name"

q3_create_table_query = construct_create_table_query(
    dict_to_insert_string(q3_cols), primary_key=q3_primary_key
)

#%%
# --------- Execute insert statement ----------------

# map table names to queries
table_create_mapper = dict(
    zip(
        (table_q1, table_q2, table_q3),
        (q1_create_table_query, q2_create_table_query, q3_create_table_query),
    )
)

table_business_statements_mapper = dict(
    zip(
        (table_q1, table_q2, table_q3),
        (query_statement_1, query_statement_1, query_statement_3),
    )
)


# ### Drop table before creating if if it exists
[drop_table(table, session) for table in (table_q1, table_q2, table_q3)]

# loop over table names in dict to create tables
for table_name in table_create_mapper.keys():
    create_table(
        table_name, session, table_create_mapper, table_business_statements_mapper
    )
#%%

#  ---------- INSERT DATA FOR 3 QUERIES INTO DB  ---------------
# avoid camelCase, use snake_case because python.
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


# read csv file with pandas - chunksize provides fixed mem use
#  pandas will automatically cast dtypes to a suitable format
df_songdata_chunks = pd.read_csv(FILE, chunksize=100000)

# loop over the chunks using itertuples to preserve dtypes as described
# in pandas docs for iterrows
# run insert queries
for chunk in df_songdata_chunks:
    for row in chunk.itertuples(index=False):
        # Query 1
        insert_music_library_col(row, q1_cols, table_q1, session, col_name_file_map)

        # Query 2
        insert_music_library_col(row, q2_cols, table_q2, session, col_name_file_map)

        # Query 3
        insert_music_library_col(row, q3_cols, table_q3, session, col_name_file_map)
#%%
# ------------- test 3 queries - will print to terminal ---------
test_query_1 = (
    "SELECT * FROM music_library_q1 WHERE session_id = 338 AND item_in_session = 4"
)
test_query_2 = "SELECT * FROM music_library_q2 WHERE user_id = 10 AND session_id = 182 ORDER BY item_in_session "
test_query_3 = "SELECT first_name, last_name FROM music_library_q3 where song = 'All Hands Against His Own'"

# run the test queries
for query in [test_query_1, test_query_2, test_query_3]:
    test_query(query, session)

#%%
# --------------- drop the tables, close session -----------------------
for table_name in table_create_mapper.keys():
    drop_table(table_name, session)

#  close session and cluster
session.shutdown()
cluster.shutdown()
#%%