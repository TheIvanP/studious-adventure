import pandas as pd
from cassandra.cluster import Cluster

from helpers import (
    drop_table,
    insert_music_library_col,
    process_files,
    test_query,
    create_table,
)

# ----------- process csv files ----------------------------------
process_files()

# ----------- set up cassandra  connection and keyspace --------------

# instantiate cassandra cluster object
cluster = Cluster()

# create a session by connecting to it. assuming localhost connection
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

# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.
# ## Create queries to ask the following three questions of the data

# ### Drop table before creating if if it exists
table_q1 = "music_library_q1"
table_q2 = "music_library_q2"
table_q3 = "music_library_q3"
drop_table((table_q1, table_q2, table_q3), session)
#%%
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
table_create_q1 = """(artist text, song text, item_in_session int, length float, session_id int, 
                        PRIMARY KEY (session_id, item_in_session))"""

# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
table_create_q2 = """(artist text, song text, first_name text, last_name text, item_in_session int, length float, level text, location text, session_id int, user_id int, 
                        PRIMARY KEY (user_id, session_id, item_in_session))"""

# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
table_create_q3 = """(song text, first_name text, last_name text, 
                        PRIMARY KEY (song, first_name, last_name))"""

table_create_mapper = dict(
    zip(
        (table_q1, table_q2, table_q3),
        (table_create_q1, table_create_q2, table_create_q3),
    )
)

for table_name in table_create_mapper.keys():
    create_table(table_name, session, table_create_mapper)
#%%

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


#  ---------- INSERT DATA FOR 3 QUERIES INTO DB  ---------------
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

# read csv file with pandas - chunksize provides fixed mem use
#  pandas will automatically cast dtypes to a suitable format
file = "event_datafile_new.csv"
df_songdata_chunks = pd.read_csv(file, chunksize=100000)

# loop over the chunks using itertuples to preserve dtypes as described
# in pandas docs for iterrows
# run insert queries
for chunk in df_songdata_chunks:
    for row in chunk.itertuples(index=False):
        # Query 1
        insert_music_library_col(
            row, q1_cols, q1_table_name, session, col_name_file_map
        )

        # Query 2
        insert_music_library_col(
            row, q2_cols, q2_table_name, session, col_name_file_map
        )

        # Query 3
        insert_music_library_col(
            row, q3_cols, q3_table_name, session, col_name_file_map
        )
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
query = "drop table music_library_q1"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "drop table music_library_q2"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "drop table music_library_q3"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


#  close session and cluster
session.shutdown()
cluster.shutdown()
#%%