from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSVToParquet").getOrCreate()

moviesDF = spark.read.csv("hdfs://master:9000/project/movies.csv")
movie_genresDF = spark.read.csv("hdfs://master:9000/project/movie_genres.csv")
ratingsDF = spark.read.csv("hdfs://master:9000/project/ratings.csv")

moviesDF.write.parquet("hdfs://master:9000/project/movies.parquet")
movie_genresDF.write.parquet("hdfs://master:9000/project/movie_genres.parquet")
ratingsDF.write.parquet("hdfs://master:9000/project/ratings.parquet")
