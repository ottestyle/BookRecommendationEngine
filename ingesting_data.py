import os
import psycopg2
import pandas as pd

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
CREATE TABLE IF NOT EXISTS books (
    id SERIAL PRIMARY KEY,
    pages INT,
    title TEXT,
    book_id BIGINT,
    rating NUMERIC(2,1),
    release_year INT,
    description TEXT,
    book_image TEXT
    );
"""

cur.execute(query_table_books)

# SQL INSERT statement
query_insert_books = """
INSERT INTO books (pages, title, book_id, rating, release_year, description, book_image)
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
query_table_book_tags = """
CREATE TABLE IF NOT EXISTS book_tags (
    id SERIAL PRIMARY KEY,
    book_id BIGINT,
    tag_id INT[]
    );
"""

cur.execute(query_table_book_tags)

# INSERT statement book_tags
query_insert_book_tags = """
INSERT INTO book_tags (book_id, tag_id)
VALUES (%s, %s);
"""

for key, df in books_tags_series[1].items():
    for _, row in df.iterrows():
        clean_values = (row["id"], row["TagId"])
        cur.execute(query_insert_book_tags, clean_values)
        
conn.commit()

###############
# Book Series #
###############
query_table_book_series = """
CREATE TABLE IF NOT EXISTS book_series (
    id SERIAL PRIMARY KEY,
    book_id BIGINT,
    position INT,
    related_book_id BIGINT
    );
"""

cur.execute(query_table_book_series)

# INSERT statement book_series
query_insert_book_series = """
INSERT INTO book_series (book_id, position, related_book_id)
VALUES (%s, %s, %s);
"""
    
for key, df in books_tags_series[2].items():
    for _, row in df.iterrows():
        position = row["Position"]
        if pd.isna(position):
            position = None
        clean_values = (row["BookId"], position, row["RelatedBookId"])
        cur.execute(query_insert_book_series, clean_values)

conn.commit()    

###########
# Authors #
###########
query_table_authors = """
CREATE TABLE IF NOT EXISTS authors (
    id SERIAL PRIMARY KEY,
    author_id BIGINT,
    name TEXT,
    author_bio TEXT,
    born_year INT,
    author_image TEXT
    );
"""

cur.execute(query_table_authors)

# INSERT statement authors
query_insert_authors = """
INSERT INTO authors (author_id, name, author_bio, born_year, author_image)
VALUES (%s, %s, %s, %s, %s);
"""

for key, df in authors.items():
    for _, row in df.iterrows():
        clean_values = []
        for v in row:
            clean_values.append(None if pd.isna(v) else v)
        cur.execute(query_insert_authors, tuple(clean_values))
        
conn.commit() 
            
########
# Tags #
########
query_table_tags = """
CREATE TABLE IF NOT EXISTS tags (
    id SERIAL PRIMARY KEY,
    category TEXT,
    tag_id INT,
    tag_name TEXT
    );
"""

cur.execute(query_table_tags)

# INSERT statement tags
query_insert_tags = """
INSERT INTO tags (category, tag_id, tag_name)
VALUES (%s, %s, %s);
"""

for key, df, in tags.items():
    for _, row in df.iterrows():
        clean_values = (key, row["TagId"], row["TagName"])
        cur.execute(query_insert_tags, clean_values)

conn.commit() 
    
# Close cursor and connection
cur.close()
conn.close()
