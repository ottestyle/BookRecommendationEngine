import csv
import pandas as pd
import sys
import ast
import re
from langdetect import detect, LangDetectException
from collections import defaultdict

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
            
    books_raw = []
    with open(f"{filepath}/books.csv", "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if row: # Ensuring row is not empty
                genre = row[1]
                book_str = row[2]
                try:
                    # Saved the CSV file where the second column was JSON and became a string
                    book_info = ast.literal_eval(book_str)
                except Exception as e:
                    print(f"Error parsing books for {genre}: {e}")
                    book_info = []
                books_raw.append({
                    "genre": genre,
                    "info": book_info
                    })
    
    # Returning [] instead of KeyError if a genre doesn't exist yet
    books_by_genre = defaultdict(list)
    for book in books_raw:
        books_by_genre[book["genre"]].append(book["info"])
    books_by_genre = dict(books_by_genre)
        
    return books_by_genre

################################################
# Books, Book Authors, Book Tags & Book Series #
################################################
def clean_books_tags_series(books):
    books_cleaned = {}
    book_author_cleaned = {}
    book_tags_cleaned = {}
    book_series_cleaned = {}
    
    # Used for finding letters in the English alphabet
    check_letters = re.compile(r"[A-Za-z]")
    
    for key, vals in books.items():
        df = pd.DataFrame(vals)
        
        # Drop empty titles
        df["title"] = df["title"].str.strip()
        df = df[df["title"] != '']
        
        # Purely numerical titles
        is_digit = df["title"].str.isdigit()
        
        # Has English letters
        has_letter = df["title"].str.contains(check_letters)
        
        # is_english on titles with English letters and numerical titles
        detect_titles = (~is_digit) & has_letter
        english_titles = pd.Series(False, index=df.index)
        english_titles[detect_titles] = df.loc[detect_titles, "title"].apply(is_english)
        
        # Keep either numerical titles or English titles
        keep = is_digit | english_titles
        df = df[keep]
        
        df = df.drop_duplicates("id")
        
        #########
        # Books #
        #########
        df_books = df[[
            "pages", "title", "id", "rating", "release_year", "description", 
            "created_at", "ratings_count", "reviews_count", "editions_count",
            "lists_count", "users_read_count"
            ]].copy()
        df_books["pages"] = (
            # non-numbers to NaN and NaN to NA
            pd.to_numeric(df_books["pages"], errors="coerce").astype("Int64") 
            )
        df_books["rating"] = (
            pd.to_numeric(df_books["rating"], errors="coerce").astype("Float64").round(1)
            )
        df_books["created_at"] = pd.to_datetime(df_books["created_at"]).dt.date
        df_books["book_image"] = df["image"].apply(
            lambda img: img.get("url") if isinstance(img, dict) else None
            )
        
        books_cleaned[key] = df_books
        
        ##################
        # Book Author #
        ##################
        df_book_authors = df[["id"]].copy()
        df_book_authors["book_author_id"] = df["contributions"].apply(
            lambda author_list: [author_dict["author_id"] for author_dict in author_list]
            if isinstance(author_list, list) else []
            )
        
        book_author_cleaned[key] = df_book_authors
        
        #############
        # Book Tags #
        #############
        df_book_tags = df[["id"]].copy()
        df_book_tags["tag_id"] = df["taggings"].apply(
            lambda tag_list: [tag_dict["tag_id"] for tag_dict in tag_list]
            if isinstance(tag_list, list) else []
            )
        
        book_tags_cleaned[key] = df_book_tags
        
        ###############
        # Book Series #
        ###############
        df_temp = df[df["book_series"].apply(lambda series_list: len(series_list) > 0)]
        df_book_series = df_temp[["id"]].rename(columns={"id": "book_id"})
        
        # Raw position
        raw_pos = df_temp["book_series"].apply(lambda series_list: series_list[0]["position"])
        df_book_series["position"] = (
            # Coerce to numeric (if a book has a position with a decimal it gets treated as NaN)
            pd.to_numeric(raw_pos, errors="coerce").round(0).astype("Int64")
            )
        
        df_book_series["related_book_id"] = df_temp["book_series"].apply(lambda series_list: series_list[0]["series"]["id"])
        
        book_series_cleaned[key] = df_book_series
        
    return {
        "books": books_cleaned, 
        "book_authors": book_author_cleaned, 
        "book_tags": book_tags_cleaned, 
        "book_series": book_series_cleaned
        }

###########    
# Authors #
###########
def clean_authors(filepath, book_author_cleaned):
    authors_cleaned = {}
    df_authors = pd.read_csv(f"{filepath}/authors.csv", usecols=["genre", "author"])
    
    for genre in df_authors["genre"].unique():
        author_genre = df_authors[df_authors["genre"] == genre]["author"].tolist()
        author_list = []
        for author in author_genre:
            temp_dict = ast.literal_eval(author)
            temp_author = {
                "author_id": temp_dict["id"],
                "name": temp_dict["name"],
                "author_bio": temp_dict["bio"],
                "born_year": temp_dict["born_year"],
                "author_image": temp_dict["image"]["url"] if temp_dict["image"] is not None and "url" in temp_dict["image"] else None
                }     
            author_list.append(temp_author)
        
        df_temp = pd.DataFrame(author_list).drop_duplicates()
        
        #### Matching author_id with cleaned books
        df_book_author_cleaned = book_author_cleaned[genre]
        
        # Explode into one row per book
        exploded = df_book_author_cleaned.explode("book_author_id")
        
        # Unique IDs
        book_author_ids = exploded["book_author_id"].unique()
        
        mask = df_temp["author_id"].isin(book_author_ids)
        
        df_matched_temp = df_temp[mask].copy()
        
        df_matched_temp["born_year"] = (
            pd.to_numeric(df_temp["born_year"], errors="coerce").astype("Int64")
            )
        authors_cleaned[genre] = df_matched_temp
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
                "tag_id": row[1], 
                "tag_name": row[2], 
                "category": row_3["category"], 
                "category_id": row_3["id"]
            })
    return tags_raw

################
# Tags Cleaned #
################
def clean_tags(tags_raw):
    df_raw = pd.DataFrame(tags_raw)
    tags = {}
    for c in df_raw["category"].unique():
        if c in ("Easiness", "Member", "Pace", "Queer", "note", "quote"):
            continue
        df_temp = df_raw[df_raw["category"] == c].copy()
        tags[c] = df_temp[["tag_id", "tag_name", "category", "category_id"]]
    return tags