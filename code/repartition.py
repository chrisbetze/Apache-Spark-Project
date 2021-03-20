from pyspark.sql import SparkSession
from io import StringIO
import csv

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]


def both_tags(x):
    l = False
    r = False

    for t in x[1]:
        if t[-1] == 'r':
            r = True
        if t[-1] == 'l':
            l = True
        if l and r:
            break

    return l and r


def detag(l):
    rs = []
    ls = []

    for elem in l[1]:
        if elem[1] == 'r':
            rs.append(elem[0])
        else:
            ls.append(elem[0])

    return (l[0], (rs, ls))


def repartition(L, R):
    l = L.map(lambda x: (x[0], (x[1:], "l")))
    r = R.map(lambda x: (x[0], (x[1:], "r")))

    res = l.union(r).groupByKey().map(lambda x: (x[0], list(x[1]))). \
        filter(both_tags).map(detag). \
        flatMap(lambda x: ((x[0], (y, x[1][1])) for y in x[1][0])). \
        flatMap(lambda x: ((x[0], (x[1][0], y)) for y in x[1][1]))

    return res



spark = SparkSession.builder.appName("Repartition").getOrCreate()
sc = spark.sparkContext

movie_genres = sc.textFile("hdfs://master:9000/project/movie_genres.csv")
movie_genres = sc.parallelize(movie_genres.take(10)).map(lambda x: (split_complex(x)[0], split_complex(x)[1]))
ratings = sc.textFile("hdfs://master:9000/project/ratings.csv").map(lambda x: (split_complex(x)[1], split_complex(x)[0], split_complex(x)[2]))

res = repartition(movie_genres, ratings)

for i in res.collect():
    print(i)

