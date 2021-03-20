from pyspark.sql import SparkSession
from io import StringIO
import csv, time

spark = SparkSession.builder.appName("Q5_RDD").getOrCreate()

sc = spark.sparkContext

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

t = time.time()

movies = sc.textFile("hdfs://master:9000/project/movies.csv"). \
    map(lambda x : (split_complex(x)[0], (split_complex(x)[1], float(split_complex(x)[7]))))

genres = sc.textFile("hdfs://master:9000/project/movie_genres.csv"). \
    map(lambda x : (x.split(",")[0], x.split(",")[1]))

ratings = sc.textFile("hdfs://master:9000/project/ratings.csv"). \
    map(lambda x : (x.split(",")[1], (x.split(",")[0], float(x.split(",")[2]))))

user_genres_maxRatings = ratings.join(genres). \
    map(lambda x : ((x[1][0][0], x[1][1]), (x[0], x[1][0][1], 1))). \
    reduceByKey(lambda x,y : (x[0], x[1], x[2] + y[2])). \
    map(lambda x : (x[0][1], (x[0][0], x[1][2]))). \
    groupByKey(). \
    mapValues(lambda x : sorted(x, key = lambda x : x[1])[-1]). \
    map(lambda x: ((x[1][0], x[0]), x[1][1]))


cinephiles = user_genres_maxRatings.map(lambda x: (x[0][0], 0)).keys().distinct().collect()

user_ratings = ratings.filter(lambda x: x[1][0] in cinephiles)

user_genre_movie_rating = user_ratings.join(genres). \
    map(lambda x: (x[0], (x[1][0][0], x[1][1], x[1][0][1]))). \
    join(movies). \
    map(lambda x: ((x[1][0][0], x[1][0][1]), (x[0], x[1][1][0], x[1][0][2], x[1][1][1]))). \
    groupByKey()

user_genre_movie_maxRating = user_genre_movie_rating. \
    mapValues(lambda x: sorted(x, key=lambda x: (x[2], x[3]))[-1])

user_genre_movie_minRating = user_genre_movie_rating. \
    mapValues(lambda x: sorted(x, key=lambda x: (-x[2], x[3]))[-1])

final = user_genre_movie_maxRating.join(user_genres_maxRatings). \
    join(user_genre_movie_minRating). \
    map(lambda x: (x[0][1], (x[0][0], x[1][0][1], x[1][0][0][1], x[1][0][0][2], x[1][1][1], x[1][1][2]))). \
    sortByKey(). \
    map(lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5]))

r = final.collect()
print("Execution Time: {:.3f}.\n".format(time.time() - t))

for i in r:
    print(i)
