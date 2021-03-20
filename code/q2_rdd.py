from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("Q2_RDD").getOrCreate()
sc = spark.sparkContext

t = time.time()

average_ratings = sc.textFile("hdfs://master:9000/project/ratings.csv"). \
        map(lambda x: (x.split(',')[0], (float(x.split(',')[2]), 1))). \
        reduceByKey(lambda x, y : (x[0] + y[0], x[1] + y[1])). \
        map(lambda x: (x[0], x[1][0] / x[1][1]))

total_ratings = average_ratings.count()

good_ratings = average_ratings.filter(lambda x: x[1] > 3). \
        count()

ratio = good_ratings / total_ratings

print("Execution Time: {:.3f}.\n".format(time.time() - t))
print("{:%}".format(ratio))
