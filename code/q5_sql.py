from pyspark.sql import SparkSession
import sys, time

file_type = sys.argv[1]

spark = SparkSession.builder.appName("Q5_SQL").getOrCreate()

if (file_type == "parquet"):
    genres = spark.read.parquet("hdfs://master:9000/project/movie_genres.parquet")
    movies = spark.read.parquet("hdfs://master:9000/project/movies.parquet")
    ratings = spark.read.parquet("hdfs://master:9000/project/ratings.parquet")
else:
    genres = spark.read.format("csv"). \
            options(header="false", inferSchema="true"). \
            load("hdfs://master:9000/project/movie_genres.csv")
    movies = spark.read.format("csv"). \
            options(header="false", inferSchema="true"). \
            load("hdfs://master:9000/project/movies.csv")
    ratings = spark.read.format("csv"). \
            options(header="false", inferSchema="true"). \
            load("hdfs://master:9000/project/ratings.csv")

genres.registerTempTable("genres")
movies.registerTempTable("movies")
ratings.registerTempTable("ratings")


genre_user_rating = \
    "select g._c1 as Genre, r._c0 as User, count(r._c0) as CountRatings " + \
    "from genres as g inner join ratings as r on r._c1 = g._c0 " + \
    "group by r._c0, g._c1"

genres_maxRatings = \
    "select a.Genre, max(a.CountRatings) as CountRatings from (" + \
    genre_user_rating + ") as a " + \
    "group by a.Genre order by a.Genre"

genres_user_maxRatings = \
    "select a.Genre, b.User, a.CountRatings from (" + \
    genres_maxRatings + ") as a inner join (" + \
    genre_user_rating + ") as b on a.Genre = b.Genre and a.CountRatings = b.CountRatings"

###################################################################################

cinephiles = \
    "select User from (" + genres_user_maxRatings + ")"

user_ratings = \
    "select r._c0 as User, r._c1 as Movie, r._c2 as Rating " + \
    "from ratings as r " + \
    "where r._c0 in (" + cinephiles + ")"

user_genre_movie_rating = \
    "select a.User, g._c1 as Genre, m._c0 as Movie, m._c1 as Title, m._c7 as Popularity, a.Rating as Rating from genres as g " + \
    "inner join (" + user_ratings + ") as a on g._c0 = a.Movie " + \
    "inner join movies as m on m._c0 = a.Movie"

user_genre_maxRating = \
    "select a.User, a.Genre, max(a.Rating) as MaxRating from (" + \
    user_genre_movie_rating + ") as a " + \
    "group by a.User, a.Genre"

user_genre_movie_maxRating = \
    "select a.User, a.Genre, a.MaxRating, b.Movie, b.Title, b.Popularity from (" + \
    user_genre_maxRating + ") as a inner join (" + \
    user_genre_movie_rating + ") as b on " + \
    "a.User = b.User and a.Genre = b.Genre and a.MaxRating = b.Rating"

mp_user_genre_movie_maxRating = \
    "select User, Genre, Movie, Title, MaxRating from (" + \
    user_genre_movie_maxRating + ") where " + \
    "(User, Genre, cast(Popularity as float)) in (select User, Genre, max(cast(Popularity as float)) " + \
    "from (" + user_genre_movie_maxRating + ") group by User, Genre)"

user_genre_minRating = \
    "select a.User, a.Genre, min(a.Rating) as MinRating from (" + \
    user_genre_movie_rating + ") as a " + \
    "group by a.User, a.Genre"

user_genre_movie_minRating = \
    "select a.User, a.Genre, a.MinRating, b.Movie, b.Title, b.Popularity from (" + \
    user_genre_minRating + ") as a inner join (" + \
    user_genre_movie_rating + ") as b on " + \
    "a.User = b.User and a.Genre = b.Genre and a.MinRating = b.Rating"

mp_user_genre_movie_minRating = \
    "select User, Genre, Movie, Title, MinRating from (" + \
    user_genre_movie_minRating + ") where " + \
    "(User, Genre, cast(Popularity as float)) in (select User, Genre, max(cast(Popularity as float)) " + \
    "from (" + user_genre_movie_minRating + ") group by User, Genre)"


###################################################################################

final = \
    "select a.Genre, a.User, a.CountRatings, b.Title as MostFav, b.MaxRating, c.Title as LeastFav, c.MinRating " + \
    "from (" + genres_user_maxRatings + ") as a " + \
    "inner join (" + mp_user_genre_movie_maxRating + ") as b " + \
    "on a.Genre = b.Genre and a.User = b.User " + \
    "inner join (" + mp_user_genre_movie_minRating + ") as c " + \
    "on a.Genre = c.Genre and a.User = c.User " + \
    "order by a.Genre"

t = time.time()
spark.sql(final).show()
print("Execution Time: {:.3f}".format(time.time() - t))
