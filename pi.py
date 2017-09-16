from pyspark import SparkContext
from math import pow

def taylor_value(n):
	''' Returns 4.0 * the n-th value in the taylor series of arctan(1)'''
	return 4.0 * pow(-1.0,n) / float(2*n+1)


N = 1e7

# start spark context
sc = SparkContext("local", "pi")

# create resiliant desributed dataset from driver to workers.
rdd = sc.parallelize(xrange(0,int(N)) )

# tranformation only workers
values = rdd.map(taylor_value)

# action: now computes everything above, on workers and driver
pi = values.reduce(lambda a,b: a + b)

# stop spark conctext
sc.stop()

# now from driver
print "\n \n pi = " + str(pi) + "\n \n"

