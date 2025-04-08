import os
import csv
import pandas as pd
import sys
import ast

os.getcwd()
os.chdir(os.environ["BOOK_RECOMMENDATION_PATH"])

#########
# Books #
#########

# Increasing the CSV field size limit
max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)

books_raw = {}
with open("books.csv", "r", encoding="utf-8") as f:
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
            
#############
# Book tags #
#############
book_tags = {}
for key in books_raw.keys():
    tag_list = []
    for value in books_raw[key]:
        temp_tags = {
            "book_id": value["id"],
            "tag_id": [tag_dict["tag"]["id"] for tag_dict in value["taggings"]]
            }
        tag_list.append(temp_tags)
    book_tags[key] = tag_list
        
###############
# Book series #
###############
book_series = {}
for key in books_raw.keys():
    series = []
    for value in books_raw[key]:
        if len(value["book_series"]) != 0:
            temp_series = {
                "book_id": value["book_series"][0]["book_id"], 
                "position": value["book_series"][0]["position"], 
                "related_book_id": value["book_series"][0]["series"]["id"]
                }
            # if temp_series["position"] is None:
            #    temp_series["position"] = 999 # Should this be null?
            series.append(temp_series)
    book_series[key] = series

#########
# Books #
#########
books = {}
for key in books_raw.keys():
    book_list = []
    for value in books_raw[key]:
        if value["title"] == "":
            continue
        temp_book = {
            "Pages": value["pages"],
            "Title": value["title"],
            "Book_id": value["id"],
            "Rating": value["rating"],
            "Release_year": value["release_year"],
            "Description": value["description"],
            "Book_image": value["image"]["url"] if value["image"] is not None and "url" in value["image"] else None
            }
        book_list.append(temp_book)
    books[key] = pd.DataFrame(book_list).drop_duplicates()

###########    
# Authors #
###########
authors = {}
df_authors = pd.read_csv("authors.csv", usecols=["genre", "author"])

for genre in df_authors["genre"].unique():
    author_genre = df_authors[df_authors["genre"] == genre]["author"].tolist()
    author_list = []
    for author in author_genre:
        temp_dict = ast.literal_eval(author)
        temp_author = {
            "Author_id": temp_dict["id"],
            "Name": temp_dict["name"],
            "Author_bio": temp_dict["bio"],
            "Born_year": temp_dict["born_year"],
            "Author_image": temp_dict["image"]["url"] if temp_dict["image"] is not None and "url" in temp_dict["image"] else None
            }     
        author_list.append(temp_author)
    authors[genre] = pd.DataFrame(author_list).drop_duplicates()

########
# Tags #
########
tags_raw = []
with open("tags.csv", "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        row_3 = ast.literal_eval(row[3])
        tags_raw.append({
            "Tag_id": row[1], 
            "Tag_name": row[2], 
            "Category": row_3["category"], 
            "Created_at": row_3["created_at"], 
            "Category_id": row_3["id"]
            })

df_tags_raw = pd.DataFrame(tags_raw)

################
# Tags cleaned #
################
tags = {}
for c in df_tags_raw["Category"].unique():
    if c in ("Easiness", "Member", "Pace", "Queer"):
        continue
    df_temp = df_tags_raw[df_tags_raw["Category"] == c].copy()
    df_temp["Created_at"] = pd.to_datetime(df_temp["Created_at"]).dt.date
    tags[c] = df_temp[["Tag_id", "Tag_name", "Created_at"]]

