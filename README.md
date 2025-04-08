# BookRecommendationEngine
A book recommendation engine that uses data from hardcover.app as the basis for recommending a book. 

My friend and I have a book club where we meet up and discuss the book we have read. Usually, I have relied on Amazon's algorithm to recommend books based on my own purchases via the website. However, I want to make it easier and not have to go to Amazon's website each time as the website is very cumbersome.

The first site that came in mind for data was of course Goodreads but obtaining an API was not possible. Instead, I opted for hardcover.app, which have an API for the website. 

My plan is to fetch data using hardcover's API, clean the data, set up a relational database and ingest into postgresql, analyze the data, build a recommendation engine and display results in interactive web dashboard.
