# Query Driven nosql Data Modelling 
---
<b>Description</b>:

Example of using queries to model schemas for storing music streaming data using [Apache Cassandra](https://cassandra.apache.org/) though the [python driver](https://docs.datastax.com/en/developer/python-driver/3.25/security/) 


Data is extracted from a set of raw csv data based on three specific business requirements listed below. We are working with a non-relational database so we want to model our data, tables and keys in a denormalized schema to provide answers for the business questions listed below. 

To avoid repetetively having to repetedly enter data in multiple sets of queries we are abstracting away data modelling into a set of functions stored in `helpers.py` and used in `musiclib_dbman.py`. 

The main program `musiclib_dbman.py` is currently structured somewhere between a notebook and a regular python program; 'cells' can be executed in VSCode via the `#%%` magics.

---

<b>Business questions:</b>

>Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4

> Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
                        
>Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

---
<b>Prerequisites</b>:

- Docker desktop
- python 3.6+
- This repo unzipped in a directory

<b>How to run</b>:


- Build and start Cassandra instance:
    - `$docker-compose build`
    - `$docker-compose up`
    - `$python3 musiclib_dbman.py`


Etl functions will combine files found in `./event_data/` into a single csv called `./event_datafile_new.csv` file. This file is parsed chunkwise using pandas api and we loop over the rows per chunk to insert data. We run test queries to verify data. 

All logging data will be printed though stdout to terminal. 




