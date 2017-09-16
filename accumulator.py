from pyspark import SparkContext

# counts the number of odds between 0 and N

sc = SparkContext("local","accum1")

N = 1000000

# create an accumulator and initialize it to 0
odds = sc.accumulator(0)

rdd = sc.parallelize(range(N))

def countOdds(x):
	global odds # make the global variable accessible
	if x % 2 != 0:
		odds += 1


rdd.map(countOdds).collect()

count = rdd.count()
sc.stop()

print "Number of odds : " + str(odds.value)

print count



