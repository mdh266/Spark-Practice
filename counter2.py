from pyspark import SparkContext

sc = SparkContext("local","counter2")

sc.textFile("text.txt")

sumCount = nums.combineByKey((lambda x: (x,1)), # if not found, yet write it as first time
							 (lambda x,y : (x[0] + y, x[1] + 1)), # if found then just increase cnt
							 (lambda x,y : (x[0] + y[0], x[1] + y[1])), # merge the accumulators
							 )

# get the average and collect it as a map
result = sumCount.map(lambda key, xy : (key, xy[0]/xy[1])).collectAsMap() 


sc.stop()

print result