import os
import psycopg2
import pandas as pd
#from psycopg2.extras import execute_values

module_path = os.environ["BOOK_RECOMMENDATION_PATH"]
data_path = os.environ["BOOK_RECOMMENDATION_DATA_PATH"]

os.chdir(module_path)

from cleaning_pre_postgresql import (
    read_books_raw, read_tags, 
    clean_books_tags_series,
    clean_authors, clean_tags
    )

books_raw = read_books_raw(data_path)
books_tags_series = clean_books_tags_series(books_raw)

authors = clean_authors(data_path)

tags_raw = read_tags(data_path)
tags = clean_tags(tags_raw)

# Connection parameters
conn_params = {
    "host": "localhost",
    "dbname": "postgres",
    "user": "postgres",
    "password": os.environ["POSTGRESQL_PW"],
    "port": 5432
    }

# Connection to postgresql server
conn = psycopg2.connect(**conn_params)

# Execute commands
cur = conn.cursor()

#########
# Books #
#########
query_table_books = """
CREATE TABLE IF NOT EXISTS Books (
    id SERIAL PRIMARY KEY,
    Pages INT,
    Title TEXT,
    BookId BIGINT,
    Rating NUMERIC(2,1),
    ReleaseYear INT,
    Description TEXT,
    BookImage TEXT
    );
"""

cur.execute(query_table_books)

# SQL INSERT statement
query_insert_books = """
INSERT INTO Books (Pages, Title, BookId, Rating, ReleaseYear, Description, BookImage)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

# Executing insert statement for books
for key, df in books_tags_series[0].items():
    for _, row in df.iterrows(): # Ignoring the first value "_"
        clean_values = []
        for v in (
                row["Pages"],
                row["Title"],
                row["BookId"],
                row["Rating"],
                row["ReleaseYear"],
                row["Description"],
                row["BookImage"],
            ):
                # Converting NA into None
                clean_values.append(None if pd.isna(v) else v)
        cur.execute(query_insert_books, tuple(clean_values))

# Commit changes
conn.commit()

#############
# Book Tags #
#############

###############
# Book Series #
###############

###########
# Authors #
###########

########
# Tags #
########

# Close cursor and connection
cur.close()
conn.close()
