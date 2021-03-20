from pyspark.sql import SparkSession
import sys, time

datatype = sys.argv[1]

spark = SparkSession.builder.appName("Q1_SQL").getOrCreate()

if datatype == 'parquet':
	movies = spark.read.parquet("hdfs://master:9000/project/movies.parquet")
else:
	movies = spark.read.format("csv"). \
		       options(header="false", inferSchema="true"). \
		       load("hdfs://master:9000/project/movies.csv")

movies.registerTempTable('movies')

sqlString = \
	"select m._c1 as Title, 100*(m._c6-m._c5)/m._c5 as Earnings, extract(year from m._c3) as Year " + \
	"from movies as m inner join (" +\
	"select max(100*(_c6-_c5)/_c5) as Earnings, extract(year from _c3) as Year "+\
	"from movies where " + \
	"_c3 is not null and _c6 is not null and _c5 is not null and _c5 != 0 and _c6 !=0 and extract(year from _c3) >= 2000 "+\
	"group by extract(year from _c3) ) as MaxProfit "+ \
	"on MaxProfit.Earnings = 100*(m._c6-m._c5)/m._c5 and MaxProfit.Year = extract(year from m._c3) " +\
	"order by extract(year from m._c3) desc"

t = time.time()
spark.sql(sqlString).show()
print("Execution Time: {:.3f}".format(time.time() - t))
