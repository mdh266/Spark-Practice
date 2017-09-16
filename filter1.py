from pyspark import SparkContext

def filter_odds(x):
	if x % 2 != 0:
		return x

# initialize spark
sc = SparkContext("local","filter1")

# create RDD
N = 100
rdd = sc.parallelize(range(N))

# transform -> filter RDD into new RDD
filtered_rdd = rdd.filter(filter_odds)

#action -> collect it on local driver
collected_rdd = filtered_rdd.collect()

# stop it 
sc.stop()


print collected_rdd