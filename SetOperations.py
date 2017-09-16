from pyspark import SparkContext


sc = SparkContext('local','SetOperations')

# declare rdds
x = sc.parallelize(set({'a','b','c','d'}))
y = sc.parallelize(set({'c','d'}))

#transformation and action
difference = x.subtract(y).collect()

union = x.union(y).collect()

intersection = x.intersection(y).collect()

c_product = x.cartesian(y).collect()

sc.stop()

print "difference"
print difference
print "union"
print union
print "intersection"
print intersection
print "cartesian product"
print c_product

