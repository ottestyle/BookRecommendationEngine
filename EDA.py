import os
from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt

engine = create_engine(f"postgresql://postgres:{os.environ['POSTGRESQL_PW']}@localhost:5432/postgres")

# Top 5 authors within each genre with most user reads
q_reads_per_book_genre = """
WITH
-- one row per book, genre
book_genre_reads AS (
    SELECT
        b.book_id,
        t.tag_name                              AS genre,
        SUM(COALESCE(b.users_read_count, 0))    AS reads_per_book_genre
    FROM books AS b
    JOIN book_tags AS bt
        ON b.book_id = bt.book_id
    CROSS JOIN LATERAL UNNEST(bt.tag_id) AS tid(tag_id)
    JOIN tags AS t
        ON tid.tag_id = t.tag_id
    WHERE t.tag_name IN (
        'Biography', 'Nonfiction', 'General', 'Biography & Autobiography',
        'Science', 'Philosophy', 'Business & Economics', 'Mathematics',
        'Psychology', 'Politics', 'Computers', 'Education', 'Self-Help',
        'Health & Fitness', 'Technology & Engineering', 'Finance'
        )
    GROUP BY b.book_id, t.tag_name
    ),

-- allocating reads to each author and then aggregating
genre_author_reads AS (
    SELECT
        bgr.genre,
        a.name                          AS author_name,
        SUM(bgr.reads_per_book_genre)   AS total_reads
    FROM book_genre_reads AS bgr
    JOIN book_authors AS ba
        ON bgr.book_id = ba.book_id
    CROSS JOIN LATERAL UNNEST(ba.author_id) AS ai(author_id)
    JOIN authors AS a
        ON ai.author_id = a.author_id
    GROUP BY bgr.genre, a.name
    )

SELECT
    genre,
    author_name,
    total_reads
FROM (
      SELECT
      *,
      ROW_NUMBER() OVER (
          PARTITION BY genre
          ORDER BY total_reads DESC
          ) AS rn
      FROM genre_author_reads
      ) ranked
WHERE rn <= 5
ORDER BY genre, total_reads DESC;
"""

df_reads_per_book_genre = pd.read_sql(q_reads_per_book_genre, engine)
df_reads_per_book_genre["total_reads"] = df_reads_per_book_genre["total_reads"].astype("Int64")

for genre in df_reads_per_book_genre["genre"].unique():
    df_temp = df_reads_per_book_genre[df_reads_per_book_genre["genre"] == genre]
    
    plt.figure()
    
    plt.barh(df_temp["author_name"], df_temp["total_reads"])
    
    # Inverting y-axis
    plt.gca().invert_yaxis()
    
    plt.xlabel("Total Reads")
    plt.title(f"Top 5 Authors in {genre}")

    plt.tight_layout()
    plt.show()
