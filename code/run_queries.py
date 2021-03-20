import os

queries = [1, 2, 3, 4, 5]
apis = ["rdd", "sql"]
datatypes = ["csv", "parquet"]

for query in queries:
    for api in apis:
        if api == "rdd":
            print("Running q{}_rdd...\n".format(query))
            os.system("spark-submit q" + str(query) + "_rdd.py 1> ../outputs/q" + str(query) + "_rdd.txt 2> log")
        else:
            for datatype in datatypes:
                print("Running q{}_sql with {}...\n".format(query, datatype))
                os.system("spark-submit q" + str(query) + "_sql.py " + datatype + " 1> ../outputs/q" + str(query) +"_"+ datatype + "_sql.txt 2> log")
    print("#########################################")
