from pyspark.sql import SparkSession
from io import StringIO
import csv, time

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def get_lustrum(date):
    year = date.split("-")[0]

    if len(year) == 0 or int(year) < 2000:
        return ""

    start = str(2000 + ((int(year)-2000)//5) * 5)
    end = str(2004 + ((int(year)-2000)//5) * 5)

    return start + "-" + end


t = time.time()

spark = SparkSession.builder.appName("Q4_RDD").getOrCreate()
sc = spark.sparkContext

movie_summary = sc.textFile("hdfs://master:9000/project/movies.csv"). \
    map(lambda x: (split_complex(x)[0], (split_complex(x)[2], get_lustrum(split_complex(x)[3])))). \
    filter(lambda x: len(x[1][1]) > 0)

movie_genre = sc.textFile("hdfs://master:9000/project/movie_genres.csv"). \
    map(lambda x: (x.split(',')[0], x.split(',')[1])). \
    filter(lambda x: x[1] == 'Drama')

res = movie_summary.join(movie_genre). \
    map(lambda x: (x[1][0][1], (len(x[1][0][0].split()), 1))). \
    reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])). \
    map(lambda x: (x[0], x[1][0] / x[1][1])). \
    sortByKey(lambda x: x[0])

r = res.collect()
print("Execution Time: {:.3f}.\n".format(time.time() - t))

for i in r:
    print(i)
