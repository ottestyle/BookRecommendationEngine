import os
import psycopg2
from psycopg2.extras import execute_values
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

authors = clean_authors(data_path, books_tags_series["book_authors"])

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
    created_at DATE,
    ratings_count INT,
    reviews_count INT,
    editions_count INT,
    lists_count INT,
    users_read_count INT,
    book_image TEXT
    );
"""

cur.execute(query_table_books)

book_rows = []
for df in books_tags_series["books"].values():
    # Setting plain python objects & converting NA into None if present
    df_clean = df.astype(object).where(pd.notna(df), None)
    
    # List of tuples for each row in the dataframes
    book_rows += df_clean.itertuples(index=False, name=None)
    
# INSERT all tuples 
execute_values(
    cur,
    """
    INSERT INTO books
      (pages, title, book_id, rating, release_year, description, created_at,
       ratings_count, reviews_count, editions_count, lists_count, users_read_count, book_image)
    VALUES %s
    """,
    book_rows
)

# Commit changes
conn.commit()

################
# Book Authors #
################
query_table_book_authors = """
CREATE TABLE IF NOT EXISTS book_authors (
    id SERIAL PRIMARY KEY,
    book_id BIGINT,
    author_id BIGINT[]
    );
"""

cur.execute(query_table_book_authors)

book_authors_rows = []
for df in books_tags_series["book_authors"].values():
    df_clean = df.copy()
    book_authors_rows += df_clean.itertuples(index=False, name=None)

execute_values(
    cur,
    """
    INSERT INTO book_authors (book_id, author_id)
    VALUES %s
    """,
    book_authors_rows
    )

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

book_tags_rows = []
for df in books_tags_series["book_tags"].values():
    df_clean = df.copy()
    book_tags_rows += df_clean.itertuples(index=False, name=None)

execute_values(
    cur,
    """
    INSERT INTO book_tags (book_id, tag_id)
    VALUES %s
    """,
    book_tags_rows
    )

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

book_series_rows = []
for df in books_tags_series["book_series"].values():
    df_clean = df.astype(object).where(pd.notna(df), None)
    book_series_rows += df_clean.itertuples(index=False, name=None)

execute_values(
    cur,
    """
    INSERT INTO book_series (book_id, position, related_book_id)
    VALUES %s
    """,
    book_series_rows
    )

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

authors_rows = []
for df in authors.values():
    df_clean = df.astype(object).where(pd.notna(df), None)
    authors_rows += df_clean.itertuples(index=False, name=None)

execute_values(
    cur,
    """
    INSERT INTO authors (author_id, name, author_bio, born_year, author_image)
    VALUES %s
    """,
    authors_rows
    )

conn.commit()
            
########
# Tags #
########
query_table_tags = """
CREATE TABLE IF NOT EXISTS tags (
    id SERIAL PRIMARY KEY,
    tag_id INT,
    tag_name TEXT,
    category TEXT,
    category_id INT
    );
"""

cur.execute(query_table_tags)

tags_rows = []
for key, df in tags.items():
    df_clean = df.copy()
    tags_rows += df_clean.itertuples(index=False, name=None)

execute_values(
    cur,
    """
    INSERT INTO tags (tag_id, tag_name, category, category_id)
    VALUES %s
    """,
    tags_rows
    )

conn.commit()
