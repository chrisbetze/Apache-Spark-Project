from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from io import StringIO
import csv, time

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
    genres = []
    movie = ""

    for elem in l[1]:
        if elem[1] == 'r':
            movie = elem[0]
        else:
            genres.append(elem[0])

    return (l[0], (movie, genres))


def repartition(L, R):
    l = L.map(lambda x: (x[0], (x[1:], "l")))
    r = R.map(lambda x: (x[0], (x[1:], "r")))

    res = l.union(r).groupByKey().map(lambda x: (x[0], list(x[1]))). \
        filter(both_tags).map(detag). \
        flatMap(lambda x: ((x[0], (y, x[1][1])) for y in x[1][0])). \
        flatMap(lambda x: ((x[0], (x[1][0], y)) for y in x[1][1]))

    return res


def bc_join (sc, R, L):
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


spark = SparkSession.builder.appName("Compare").getOrCreate()
sc = spark.sparkContext

movie_genres = sc.textFile("hdfs://master:9000/project/movie_genres.csv")
movie_genres = sc.parallelize(movie_genres.take(100))
movie_genres = movie_genres.map(lambda x: (split_complex(x)[0], split_complex(x)[1]))

ratings = sc.textFile("hdfs://master:9000/project/ratings.csv").map(lambda x: (split_complex(x)[1], split_complex(x)[0], split_complex(x)[2]))

t = time.time()
rep = repartition(movie_genres, ratings).collect()
rep_time = time.time() - t

t = time.time()
bc = bc_join(sc, movie_genres, ratings).collect()
bc_time = time.time() - t

print("Time for repartition join: %.4f sec."%(rep_time)) 
print("Time for broadcast join: %.4f sec."%(bc_time)) 
