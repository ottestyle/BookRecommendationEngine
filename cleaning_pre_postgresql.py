import csv
import pandas as pd
import sys
import ast
import re
from langdetect import detect, LangDetectException

def is_english(title_text):
    """Checks if title is in English"""
    try:
        return detect(title_text) == "en"
    except LangDetectException:
        return False

#############
# Books Raw #
#############
def read_books_raw(filepath):
    # Increasing the CSV field size limit
    max_int = sys.maxsize
    while True:
        try:
            csv.field_size_limit(max_int)
            break
        except OverflowError:
            max_int = int(max_int / 10)

    books_raw = {}
    with open(f"{filepath}/books.csv", "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if row:  # Ensuring row is not empty 
                genre = row[0]
                books_str = row[1]
                try:
                    # Saved the CSV file where the second column was JSON and became a string
                    books_list = ast.literal_eval(books_str)
                except Exception as e:
                    print(f"Error parsing books for {genre}: {e}")
                    books_list = []
                books_raw[genre] = books_list
    return books_raw

#########
# Books #
#########
def clean_books(books):
    books_cleaned = {}
    check_letters = re.compile(r"[A-Za-z]")
    for key in books.keys():
        book_list = []
        for value in books[key]:
            
            # Skip book without title
            if not value["title"].strip():
                continue
            
            # Keep book with a numerical title
            if value["title"].isdigit():
                keep_book = True
            
            # Skip book without any letters ranging from A-Z (a-z) and not in English
            elif not check_letters.search(value["title"]):
                continue
            else:
                keep_book = is_english(value["title"])
            
            if not keep_book:
                continue
            
            temp_book = {
                "Pages": value["pages"],
                "Title": value["title"],
                "BookId": value["id"],
                "Rating": value["rating"],
                "ReleaseYear": value["release_year"],
                "Description": value["description"],
                "BookImage": value["image"]["url"] if value["image"] is not None and "url" in value["image"] else None
                }
            book_list.append(temp_book)
        books_cleaned[key] = pd.DataFrame(book_list).drop_duplicates()
    return books_cleaned
                
#############
# Book tags #
#############
def clean_book_tags(books):
    book_tags = {}
    for key in books.keys():
        tag_list = []
        for value in books[key]:
            temp_tags = {
                "BookId": value["id"],
                "TagId": [tag_dict["tag"]["id"] for tag_dict in value["taggings"]]
                }
            tag_list.append(temp_tags)
        book_tags[key] = tag_list
    return book_tags
        
###############
# Book series #
###############
def clean_book_series(books):
    book_series = {}
    for key in books.keys():
        series = []
        for value in books[key]:
            if len(value["book_series"]) != 0:
                temp_series = {
                    "BookId": value["book_series"][0]["book_id"], 
                    "Position": value["book_series"][0]["position"], 
                    "RelatedBookId": value["book_series"][0]["series"]["id"]
                    }
                series.append(temp_series)
        book_series[key] = series
    return book_series



###########    
# Authors #
###########
def clean_authors(filepath):
    authors_cleaned = {}
    df_authors = pd.read_csv(f"{filepath}/authors.csv", usecols=["genre", "author"])
    
    for genre in df_authors["genre"].unique():
        author_genre = df_authors[df_authors["genre"] == genre]["author"].tolist()
        author_list = []
        for author in author_genre:
            temp_dict = ast.literal_eval(author)
            temp_author = {
                "AuthorId": temp_dict["id"],
                "Name": temp_dict["name"],
                "AuthorBio": temp_dict["bio"],
                "BornYear": temp_dict["born_year"],
                "AuthorImage": temp_dict["image"]["url"] if temp_dict["image"] is not None and "url" in temp_dict["image"] else None
                }     
            author_list.append(temp_author)
        authors_cleaned[genre] = pd.DataFrame(author_list).drop_duplicates()
    return authors_cleaned

############
# Tags Raw #
############
def read_tags(filepath):
    tags_raw = []
    with open(f"{filepath}/tags.csv", "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            row_3 = ast.literal_eval(row[3])
            tags_raw.append({
                "TagId": row[1], 
                "TagName": row[2], 
                "Category": row_3["category"], 
                "CreatedAt": row_3["created_at"], 
                "CategoryId": row_3["id"]
            })
    return tags_raw

################
# Tags cleaned #
################
def clean_tags(tags_raw):
    df_raw = pd.DataFrame(tags_raw)
    tags = {}
    for c in df_raw["Category"].unique():
        if c in ("Easiness", "Member", "Pace", "Queer"):
            continue
        df_temp = df_raw[df_raw["Category"] == c].copy()
        df_temp["CreatedAt"] = pd.to_datetime(df_temp["CreatedAt"]).dt.date
        tags[c] = df_temp[["TagId", "TagName", "CreatedAt"]]
    return tags