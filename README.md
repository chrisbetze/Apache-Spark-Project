# Apache-Spark-Project

## Description
In this work, Apache Spark was used to calculate queries on files that describe datasets. Apache Spark offers two APIs for implementing queries:
1. [Resilient Distributed Dataset (RDD) API](https://spark.apache.org/docs/2.4.4/rdd-programming-guide.html)
2. [Dataframe API / Spark SQL](https://spark.apache.org/docs/2.4.4/sql-programming-guide.html)

## Dataset preview
We use a dataset with movies, which comes from a subset of the *[Full MovieLens Dataset](https://grouplens.org/datasets/movielens/latest/)*. This subset contains three text files in CSV format. The *movie_genres.csv*, *movies.csv* and *ratings.csv*.

| File  | Description |
| ------------- | ------------- |
| movies.csv  | Describes the movies in the dataset.  |
| movie_genres.csv  | Contains on each line a pair of movie ID and movie genre in first and second place respectively.  |
| ratings.csv  | Describes user ratings for movies.  |

*For more information about the files, visit [Kaggle Movie Dataset](https://www.kaggle.com/rounakbanik/the-movies-dataset)*.
## Queries

| Query  | Description |
| ------------- | ------------- |
| Q1  | From 2000 onwards, for each year, find the movie with the biggest profit. Ignore entries that have no value on the release date or zero value in revenue or budget.  |
| Q2  | Find the percentage of users (%) who have given to movies average rating greater than 3.  |
| Q3  | For each genre, find the average rating of the genre and the number of films belonging to this genre. If a movie belongs to more than one genres, we consider to be measured in each genre.  |
| Q4  | For "Drama" movies, find the average movie summary length every 5 years from 2000 onwards (1st 5 years: 2000-2004, 2nd 2005-2009, 3rd 2010-2014, 4th 2015-2019) |
| Q5  | For each genre, find the user with the most reviews along with his most and least favorite movie according to his ratings. The results should be in alphabetical order as to the genre and be presented in a table with the following columns: <br> • Genre <br> • User with more reviews <br> • Number of reviews <br> • Most favorite movie <br> • Rating of most favorite movie <br> • Least favorite movie <br> • Rating of least favorite movie |

## Part 1
1. We upload the 3 CSV files in Hadoop Distributed File System (HDFS).
2. We convert the 3 CSV files to [Parquet](https://parquet.apache.org/) format and then we save them back to hdfs.
3. We implement the 5 queries and calculate the execution time for the following 3 cases:
    * Map Reduce Queries – RDD API
    * Spark SQL with input csv file
    * Spark SQL with input parquet file
4. The execution times and the bar charts, grouped by query, displayed also in the [report](https://github.com/chrisbetze/Apache-Spark-Project/blob/cdd6ab4f85453e486c7251664714a29646b71259/report.pdf).
<img src="https://user-images.githubusercontent.com/50949470/111872098-432c2b80-8996-11eb-9dd9-c8971de009a4.PNG" width="500" height="300">

## Part 2
1. We implement the [Broadcast join](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.644.9902&rep=rep1&type=pdf) in RDD API (Map Reduce).
2. We implement the [Repartition join](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.644.9902&rep=rep1&type=pdf) in RDD API (Map Reduce).
3. We compare the execution times of the two implementations above.
<img src="https://user-images.githubusercontent.com/50949470/111873044-17f60c00-8997-11eb-8a22-0a639f076892.PNG" width="500" height="300">

4. We execute a query with and without optimizer ([Spark SQL Query Optimizer](https://spark.apache.org/docs/2.4.4/tuning.html)) and we present the results in a bar graph displayed also in the [report](https://github.com/chrisbetze/Apache-Spark-Project/blob/cdd6ab4f85453e486c7251664714a29646b71259/report.pdf).
<img src="https://user-images.githubusercontent.com/50949470/111873075-48d64100-8997-11eb-9e94-a659faa1720b.PNG" width="500" height="300">
