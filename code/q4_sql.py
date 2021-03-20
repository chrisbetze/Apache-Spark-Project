from pyspark.sql import SparkSession
import sys, time

file_type = sys.argv[1]

spark = SparkSession.builder.appName("Q4_SQL").getOrCreate()

def get_lustrum(year):
    start = str(2000 + ((year-2000)//5) * 5)
    end = str(2004 + ((year-2000)//5) * 5)

    return start + "-" + end


if (file_type == "parquet"):
    genres = spark.read.parquet("hdfs://master:9000/project/movie_genres.parquet")
    movies = spark.read.parquet("hdfs://master:9000/project/movies.parquet")
else:
    genres = spark.read.format("csv"). \
            options(header="false", inferSchema="true"). \
            load("hdfs://master:9000/project/movie_genres.csv")
    movies = spark.read.format("csv"). \
            options(header="false", inferSchema="true"). \
            load("hdfs://master:9000/project/movies.csv")

genres.registerTempTable("genres")
movies.registerTempTable("movies")
spark.udf.register("lustrum", get_lustrum)
spark.udf.register("length", lambda x: len(x.split()) if x != None else 0)


sqlString = \
    "select c.Period, sum(c.Plot) / sum(c.Count) as Average from " + \
    "(select lustrum(b.Year) as Period, b.Avg_Plot as Plot, b.Count as Count from " + \
    "(select a.Year as Year, sum(a.Plot) as Avg_Plot, count(a.Plot) as Count from " + \
    "(select m._c1 as Title, length(m._c2) as Plot, extract(year from m._c3) as Year from movies as m " + \
    "inner join genres as g on m._c0 = g._c0 and g._c1 = 'Drama' and extract(year from m._c3) >= 2000) as a " + \
    "group by a.Year) as b) as c " + \
    "group by c.Period"


t = time.time()
spark.sql(sqlString).show()
print("Execution Time: {:.3f}".format(time.time() - t))
