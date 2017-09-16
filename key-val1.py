from pyspark import SparkContext

# key-value pairs need to be in tupples for Python
key_val_pairs = {('a',1), ('b',2), ('a',5), ('b',8), ('c',3)}

# initialize spark context
sc = SparkContext("local","key_val")

# creat the rdd
rdd = sc.parallelize(key_val_pairs)

# transform it and collect it
reduced_rdd = rdd.reduceByKey(lambda x,y : x+y).collect()

grouped_rdd = rdd.groupByKey().collect()

mapped_rdd = rdd.mapValues(lambda x: x+1).collect()
# get the distinct keys
keys = rdd.keys().distinct().collect()

# stop the SPAHK
sc.stop()

print "keys"
print keys

print "reduced rdd"
print reduced_rdd

print "grouped rdd"
print grouped_rdd

print "mapped values"
print mapped_rdd

