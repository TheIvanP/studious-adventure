# Import Python packages
import os
import glob
import csv
import black


def process_files():
    # Join a set of event data stored in indiual csv file into one output file
    # for parsing

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

    # check the number of rows in your csv file
    with open("event_datafile_new.csv", "r", encoding="utf8") as f:
        print(sum(1 for line in f))


#%%
def test_query(query_in: str, session, should_limit=False, limit=5) -> None:
    """reads a query string, executes it on the given session
    and print each returned row using black formatter

    Args:
    query_in:str        query string expecting cassandra query language
    session:session     cassandra session object
    should_limit:bool   toggle for limiting number of returned rows
    limit:int           how many rows to return if limiting
    """

    query = query_in
    if should_limit:
        query = f"{query_in} LIMIT {limit}"

    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    print("-" * 50)
    print("Data Validation Query: ")
    print(" " * 50)
    print(query_in)
    print("-" * 50)
    print("Result: ")
    print(" " * 50)
    for row in rows:

        # black was chosen ahead of pandas dataframe for printing
        # as the format is more compact and better suited for low
        # numer of rows with textual content.
        print(black.format_str(repr(row), mode=black.Mode()))
    print(" " * 50)
    print("-" * 50)


def drop_table(tables: tuple, session) -> None:
    for table in tables:
        query = f"DROP TABLE IF EXISTS {table}"
        try:
            session.execute(query)
        except Exception as e:
            print(e)


def insert_music_library_col(
    row, query_cols: tuple, table_name: str, session, col_name_file_map: dict
) -> None:
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

    def _get_value_from_csv(row, key: str, col_name_file_map: dict):
        """Takes a row and extracts the value
        using the defined column mapping

        This saves us from having to deal with
        column names as defined in the csv, we can
        stick with our own naming convention

        Args:
        row:Row casssandra row object
        key:str column name to get as defined in table names
        """

        return row._asdict().get(col_name_file_map.get(key))

    #  get elements in query_cols as one string for insertion into query
    query_cols_asstring = ", ".join(query_cols)

    #  compose query insert statement
    query_insert = f"INSERT INTO {table_name} ({query_cols_asstring}) "

    # for each element in query cols create value substitution magics
    #  to avoid having to match these to the number of columns we're inserting
    subs = ", ".join(["%s" for x in query_cols])

    #  compose value insertion query string
    values = f"VALUES ({subs})"

    #  get the data from row looking up the column names
    cols = [
        _get_value_from_csv(row, col_name, col_name_file_map) for col_name in query_cols
    ]

    # execute the session, casting cols to tuple for compatability with execute method
    try:
        session.execute(
            f"{query_insert}{values}",
            tuple(cols),
        )
    except Exception as e:
        print(e)


def create_table(
    table_name: str,
    session,
    table_create_mapper: dict,
    table_business_statements_mapper: dict,
) -> None:
    """Compose a query for creating a table based on table name and
    looking up query based on table name in table_name_create_mapper

    Args:
    table_name:str          name of the table to insert. also used for looking up query
    session:                cassandra session
    table_create_mapper:    dict with names of tables to insert and queries for cols and primary keys
    query_statement:str     (optional) - query business requirement
    """
    print("-" * 50)
    query = (
        f"CREATE TABLE IF NOT EXISTS {table_name} {table_create_mapper.get(table_name)}"
    )

    print(f"Query business requirement :")
    print(f"{table_business_statements_mapper.get(table_name)}")
    print("    ")
    print(f"Query for creating tables of {table_name} :")
    print("    ")
    print(query)
    print("    ")

    try:
        session.execute(query)
    except Exception as e:
        print(e)


def drop_table(table_name: str, session) -> None:
    """Drop a table  from cassandra

    Args:
    table_name:str  name of the table to drop
    session:        cassandra session
    """

    query = f"drop table {table_name}"
    try:
        # do we have to assign here?
        rows = session.execute(query)
    except Exception as e:
        print(e)


def dict_to_insert_string(dict_in: dict, sep=", ") -> str:
    """Convert dict to sepearated string

    Args:
    dict_in:dict        pair of strings in dict
    sep:dict:optional   separator in string
    """
    try:
        vals = [[k, v] for k, v in dict_in.items()]
        vals = [" ".join(x) for x in vals]
        vals = sep.join(vals)
    except Exception as e:
        print(e)

    return vals


def construct_create_table_query(insert_pairs: str, primary_key: str) -> str:
    """Construct query for creating a table

    Args:
    insert_pairs:str    string with colname type
    primary_key_str     primary key
    """
    insert_query = f"({insert_pairs}, PRIMARY KEY ({primary_key}))"
    return insert_query