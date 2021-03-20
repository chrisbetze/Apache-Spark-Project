from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("Q3_RDD").getOrCreate()
sc = spark.sparkContext

t = time.time()

movie_genre = sc.textFile("hdfs://master:9000/project/movie_genres.csv"). \
    map(lambda x: ((x.split(',')[0], x.split(',')[1]), 1)). \
    reduceByKey(lambda x,y: y). \
    map(lambda x: (x[0][0], x[0][1]))

average_movie_rating = sc.textFile("hdfs://master:9000/project/ratings.csv"). \
    map(lambda x: (x.split(",")[1], (float(x.split(",")[2]), 1))). \
    reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1])). \
    map(lambda x: (x[0], x[1][0] / x[1][1]))

res = movie_genre.join(average_movie_rating). \
    map(lambda x: (x[1][0], (x[1][1], 1))). \
    reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])). \
    map(lambda x: (x[0], x[1][0] / x[1][1], x[1][1]))

r = res.collect()
print("Execution Time: {:.3f}.\n".format(time.time() - t))

for i in r:
    print(i)
