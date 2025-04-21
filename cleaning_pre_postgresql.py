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
            # Ensuring row is not empty
            if row:   
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

##################################
# Books, Book Tags & Book Series #
##################################
def clean_books_tags_series(books):
    books_cleaned = {}
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
        df_books = df[["pages", "title", "id", "rating", "release_year", "description"]]
        df_books = df_books.rename(columns={
            "pages": "Pages",
            "title": "Title",
            "id": "BookId",
            "rating": "Rating",
            "release_year": "ReleaseYear",
            "description": "Description",
            })
        df_books["BookImage"] = df["image"].apply(
            lambda img: img.get("url") if isinstance(img, dict) else None
            )
        
        books_cleaned[key] = df_books
        
        #############
        # Book Tags #
        #############
        df_book_tags = df[["id"]].copy()
        df_book_tags["TagId"] = df["taggings"].apply(
            lambda tag_list: [tag_dict["tag"]["id"] for tag_dict in tag_list]
            if isinstance(tag_list, list) else []
            )
        
        book_tags_cleaned[key] = df_book_tags
        
        ###############
        # Book Series #
        ###############
        df_temp = df[df["book_series"].apply(lambda series_list: len(series_list) > 0)]
        df_book_series = df_temp[["id"]].rename(columns={"id": "BookId"})
        df_book_series["Position"] = df_temp["book_series"].apply(lambda series_list: series_list[0]["position"])
        df_book_series["RelatedBookId"] = df_temp["book_series"].apply(lambda series_list: series_list[0]["series"]["id"])
        
        book_series_cleaned[key] = df_book_series
        
    return books_cleaned, book_tags_cleaned, book_series_cleaned

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
# Tags Cleaned #
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