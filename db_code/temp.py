import pandas as pd
import numpy as np
import os
import io
from sqlalchemy import *
from utilities.db import get_np_na_value, get_np_data_type, get_data_type
from config.connection import connection_string, metadata_schema, data_file_directory, raw_schema

'''
    3 - Loads the raw data from the CDC files into the raw data schema, creating the tables based on the types and
'''

# SETTINGS #
# The files_table setting indicates the name of a view in the metadata schema
# that contains the list of files that we wish to import.
files_table = 'files_2016'
fields_view_prefix = 'fields_'

chunk_size = 10000  # Records per chunk
limit_chunks = 0  # 0 means unlimited

# Instantiate DB engine
engine = create_engine(connection_string, echo=False)

# Obtain connection to metadata schema
meta = MetaData(schema=metadata_schema)

# Open connection
# The SQLAlchemy connection to the database
conn = engine.connect()
# A raw psycopg2 DB connection
raw_conn = engine.raw_connection()
# A cursor using the raw connection
cur = raw_conn.cursor()
# Text output object
output = io.StringIO()

# Get list of files based on the settings
files = Table(files_table, meta, autoload=True, autoload_with=engine)
files_view = conn.execute(select([files]))

# Loop through the files that we are importing into the database
for file in files_view:
    # Obtain file characteristics for each file from the file record.
    year = file[files.c.year]
    fraction = file[files.c.fraction]
    file_type = file[files.c.type]
    file = file[files.c.name]

    # Construct import table name from file record characteristics
    target_table = file_type.lower() + "_" + fraction.lower() + "_" + str(year)

    # Define fields view
    fields = Table(fields_view_prefix + target_table, meta, autoload=True, autoload_with=engine)

    # Select fields metadata
    fields_view = conn.execute(select([fields]))
    colSpecs = []
    colNames = []
    dTypes_db = {}
    dTypes_np = {}
    dTypes_na = {}

    # Loop through the fields for this file and build up the lists and dictionaries we need to
    # define the target tables and write them to the DB
    for row in fields_view:
        # Get the column name for each field
        col = row[fields.c.friendly_header]
        # Build the column specifications list by providing the start and end index in the fixed-width file.
        colSpecs.append([row[fields.c.start_base_zero], row[fields.c.end_base_zero]])
        # Build the column names list
        colNames.append(col)
        # Build the database column type dictionary
        dTypes_db[col] = get_data_type(row[fields.c.type], row[fields.c.length])
        # Build the numpy column type dictionary
        dTypes_np[col] = get_np_data_type(row[fields.c.type])
        # Build the nan/na value dictionary
        if row[fields.c.type] != 'VARCHAR':
            dTypes_na[col] = get_np_na_value(row[fields.c.length])

    # Grab the import file from the file system
    filename = os.path.join(data_file_directory, str(year), file_type, file)

    # grab just the first row for table creation purposes. Note that the column order is important and
    # must match the order of the fixed width file
    # See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_fwf.html for details of the
    # Pandas fixed-width file reader.
    first_row = pd.read_fwf(filename, colspecs=colSpecs, dtype=dTypes_np, header=None, names=colNames,
                            skip_blank_lines=False, na_values=dTypes_na,
                            delim_whitespace=True, nrows=1)

    # grab the rest of the data frame, all but the first row
    reader = pd.read_fwf(filename, colspecs=colSpecs, dtype=dTypes_np, header=None, names=colNames,
                         skip_blank_lines=False, na_values=dTypes_na,
                         delim_whitespace=True, chunksize=chunk_size, skiprows=1)

    # Start "chunking" through the file
    for i, chunk in enumerate(reader):
        # Create temporary CSV file in memory
        output = io.StringIO()
        # Is this the first chunk?
        if i == 0:
            # Create empty table
            # Replace empty fields with proper numpy null value
            first_row.replace(r'^\s*$', np.nan, regex=True, inplace=True)
            # Set the index field at 1,000,000 and increment by one for the first row
            first_row.insert(0, 'id', range(1000000, 1000001))
            # Write first row to DB which will also generate the target table based on the metadata
            # if the table already exists, replace it.
            first_row.to_sql(target_table, con=conn, schema=raw_schema,
                             index=False, dtype=dTypes_db,
                             if_exists='replace')
        # Replace all blank cells with NaN
        # Define the current input row index
        index = i * chunk_size + 1000001
        # Replace empty fields with proper numpy null value
        chunk.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        # Set the index field for the subsequent inserts
        chunk.insert(0, 'id', range(index, index + len(chunk)))
        # Strange syntax (see: https://pandas.pydata.org/pandas-docs/stable/generated/pandas.notnull.html)
        # This is replacing all NaN with the string 'Null'
        chunk = chunk.where((pd.notnull(chunk)), 'Null')
        # Converts data frame to CSV in memory
        chunk.to_csv(output, sep='\t', header=False, index=False)
        # Set reader to beginning of in-memory csv
        output.seek(0)
        # Start reading contents of in-memory csv
        contents = output.getvalue()
        # writes rows to DB using COPY command on raw psycopg2 connection, much faster than pandas' to_sql
        cur.copy_from(output, raw_schema + '.' + target_table, null='Null')
        # Commit inserts to DB
        raw_conn.commit()
        print(file + ', ' + file_type + ", " + str(year) + ', ' + fraction + ': ' + str((i + 1) * chunk_size))
        if limit_chunks > 0:
            if i == limit_chunks - 1:
                break
