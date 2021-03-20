from pyspark.sql import SparkSession
from io import StringIO
import csv, time

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def get_year(date):
    return date.split("-")[0]

def gains(cost, rev):
    if cost > 0:
        return 100*(rev - cost) / cost
    else:
        return -1

spark = SparkSession.builder.appName("Q1_RDD").getOrCreate()
sc = spark.sparkContext

t = time.time()

res = sc.textFile("hdfs://master:9000/project/movies.csv"). \
        map(lambda x: ((get_year(split_complex(x)[3])), (split_complex(x)[1], gains(float(split_complex(x)[5]), float(split_complex(x)[6]))))). \
        filter(lambda x: x[0].isnumeric() and int(x[0]) >= 2000 and x[1][1] > -1). \
        groupByKey(). \
        mapValues(lambda x: sorted(x, key = lambda y: y[1], reverse=True)[0]). \
        map(lambda x: (x[0], (x[1][0], x[1][1]))). \
        sortByKey(lambda x: x[0])

r = res.collect()
print("Execution Time: {:.3f}.\n".format(time.time() - t))

for i in r:
    print(i)
