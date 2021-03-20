from pyspark.sql import SparkSession
import sys, time

datatype = sys.argv[1]

spark = SparkSession.builder.appName("Q3_SQL").getOrCreate()

if (datatype == 'parquet'):
        genres = spark.read.parquet("hdfs://master:9000/project/movie_genres.parquet")
        ratings = spark.read.parquet("hdfs://master:9000/project/ratings.parquet")
else:
        genres = spark.read.format("csv"). \
                       options(header="false", inferSchema="true"). \
                       load("hdfs://master:9000/project/movie_genres.csv")
        ratings = spark.read.format("csv"). \
                        options(header="false", inferSchema="true"). \
                        load("hdfs://master:9000/project/ratings.csv")

genres.registerTempTable("genres")
ratings.registerTempTable("ratings")

sqlString = \
	"select g._c1 as Genre, AVG(a.Average) as Average, count(distinct g._c0) as MoviesCount " + \
	"from (select r._c1, avg(r._c2) as Average from ratings as r group by r._c1) as a " + \
        "inner join genres as g on a._c1 = g._c0 group by g._c1"


t = time.time()
spark.sql(sqlString).show()
print("Execution Time: {:.3f}".format(time.time() - t))
