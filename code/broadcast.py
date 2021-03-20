from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from io import StringIO
import csv


def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


spark = SparkSession.builder.appName("Broadcast").getOrCreate()
sc = spark.sparkContext


def bc_join (R, L):
    # let's assume R << L

    R = R.collect()
    bc = sc.broadcast(R)
    bc_dict = dict()

    for key, value in bc.value:
        if key not in bc_dict:
            bc_dict[key] = [value]
        else:
            bc_dict[key].append(value)

    def foo(x):
        if x[0] in bc_dict:
            return (x[0], x[1:], bc_dict[x[0]])
        else:
            return ("", "", "")

    res = L.map(foo). \
    filter(lambda x: x[0] != ""). \
    flatMap(lambda x: ((x[0], (x[1], y)) for y in x[2]))

    return res 

movie_genres = sc.textFile("hdfs://master:9000/project/movie_genres.csv")
movie_genres = sc.parallelize(movie_genres.take(100))
movie_genres = movie_genres.map(lambda x: (split_complex(x)[0], split_complex(x)[1]))

ratings = sc.textFile("hdfs://master:9000/project/ratings.csv").map(lambda x: (split_complex(x)[1], split_complex(x)[0], split_complex(x)[2]))

res = bc_join(movie_genres, ratings)

for i in res.collect():
    print(i)
