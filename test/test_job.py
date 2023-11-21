import pyspark

sc = pyspark.SparkContext()
rdd = sc.textFile("gs://landing_5333/inputtest.csv")
print(sorted(rdd.collect()))