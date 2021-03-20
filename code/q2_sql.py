from pyspark.sql import SparkSession
import sys, time

datatype = sys.argv[1]

spark = SparkSession.builder.appName("Q2_SQL").getOrCreate()

if (datatype == 'parquet'):
        ratings = spark.read.parquet("hdfs://master:9000/project/ratings.parquet")
else:       #Csv by Default
        ratings = spark.read.format("csv"). \
                        options(header="false", inferSchema="true"). \
                        load("hdfs://master:9000/project/ratings.csv")

ratings.registerTempTable('ratings')

sqlString = \
	"select 100*(select distinct count(*) from ("+\
	"select r._c0, avg(r._c2) from ratings as r "+\
	"group by r._c0 having avg(r._c2) > 3.0)) / (select count (distinct _c0) from ratings) as Perentage"


t = time.time()
spark.sql(sqlString).show()
print("Execution Time: {:.3f}".format(time.time() - t))
