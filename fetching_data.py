import requests
import pandas as pd
import time
import os
import csv

data_path = os.environ["BOOK_RECOMMENDATION_DATA_PATH"]

os.chdir(data_path)

url = "https://api.hardcover.app/v1/graphql"

headers = {
    "Content-Type": "application/json",
    "authorization": os.environ["HARDCOVER_AUTH"],
    "User-Agent": requests.utils.default_user_agent()
    }

genres = [
    "Biography", "Nonfiction", "General", "Biography & Autobiography", 
    "Science", "Philosophy", "Business & Economics", "Mathematics", 
    "Psychology", "Politics", "Computers", "Education", "Self-Help", 
    "Health & Fitness", "Technology & Engineering", "Finance"
]

def make_request(query, variables=None, max_retries=5, timeout=30):
    """Function to handle timeouts and exceptions"""
    retries = 0
    success = False
    while retries < max_retries and not success:
        try:
            response = requests.post(url, json={"query": query, "variables": variables},
                                     headers=headers, timeout=timeout)
            data = response.json()
            success = True
            return data
        except requests.exceptions.Timeout:
            retries += 1
            print(f"Request timed out. Retry {retries}/{max_retries}")
            time.sleep(5)
        except Exception as e:
            print("An error occurred:", e)
            break
    return None

###################
# Books & Authors # 
###################

# Store results by genre
all_books = {}
all_authors = {}

for genre in genres:
    print(f"Processing genre: {genre}")
    offset = 0
    limit = 100
    books = []
    
    # Books
    while True:
        query_books = f"""
        query Books($offset: Int, $limit: Int) {{
          books(
            where: {{taggings: {{tag: {{tag: {{_eq: "{genre}"}}}}}}}},
            order_by: {{title: asc}},
            offset: $offset,
            limit: $limit
          ) {{
            pages
            title
            id
            rating
            release_year
            description
            created_at
            ratings_count
            reviews_count
            editions_count
            lists_count
            users_read_count
            contributions {{
                author_id
                }}
            book_series {{
              book_id
              featured
              position
              series {{
                id
                name
              }}
            }}
            image {{
              url
            }}
            taggings {{
              tag_id
            }}
          }}
        }}
        """
        variables = {"offset": offset, "limit": limit}
        data = make_request(query_books, variables=variables)
        if data is None:
            print("Request failed; exiting books loop for this genre.")
            break

        result = data.get("data", {}).get("books")
        if not result:
            print(f"No more books returned for genre {genre} at offset {offset}.")
            break

        books.extend(result)
        offset += limit
        print("Books offset:", offset)
        time.sleep(1)

    all_books[genre] = books

    # Authors
    offset_authors = 0
    limit_authors = 100
    authors = []
    
    while True:
        query_authors = f"""
        query Authors($offset: Int, $limit: Int) {{
          authors(
            where: {{contributions: {{book: {{taggings: {{tag: {{tag: {{_eq: "{genre}"}}}}}}}}}}}},
            order_by: {{name: asc}},
            offset: $offset,
            limit: $limit
          ) {{
            id
            name
            bio
            born_year
            image {{
              url
            }}
          }}
        }}
        """
        variables = {"offset": offset_authors, "limit": limit_authors}
        data = make_request(query_authors, variables=variables)
        if data is None:
            print("Authors request failed for genre:", genre)
            break
    
        result = data.get("data", {}).get("authors")
        if not result:
            print(f"No more authors returned for genre {genre} at offset {offset_authors}.")
            break
    
        authors.extend(result)
        offset_authors += limit_authors
        print("Authors offset:", offset_authors)
        time.sleep(1)
    
    all_authors[genre] = authors

df_books = pd.DataFrame([(genre, book) for genre, books in all_books.items() for book in books],
                        columns=["genre", "book"])
df_authors = pd.DataFrame([(genre, author) for genre, authors in all_authors.items() for author in authors],
                          columns=["genre", "author"])

df_books.to_csv("books.csv")
df_authors.to_csv("authors.csv")

########
# Tags #
########
all_tags = []
offset_tags = 0
limit_tags = 100  

while True:
    query_tags = """
    query Tags($offset: Int, $limit: Int) {
      tags(
        offset: $offset,
        limit: $limit
      ) {
        id
        tag
        tag_category {
            category
            id
            }
      }
    }
    """
    variables = {"offset": offset_tags, "limit": limit_tags}
    data = make_request(query_tags, variables=variables)
    if data is None:
        print(f"Tags request failed for {offset_tags}")
        break
    
    result = data.get("data", {}).get("tags")
    if not result:
        print(f"No more tags returned at offset {offset_tags}.")
        break
    
    all_tags.extend(result)
    offset_tags += limit_tags
    print("Tags offset:", offset_tags)
    time.sleep(1)

df_tags = pd.DataFrame(all_tags)
df_tags.to_csv("tags.csv")
