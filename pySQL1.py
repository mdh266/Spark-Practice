from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext('local','pySQL1')
sql_sc = SQLContext(sc)

data = [('alice', 1), ('bob', 2), ('Joe', 3)]

data2 = [('alice', 1), ('bob', 2), ('Joe', 3), ('bob', 2)]


# nothing done here, spark just records how to build df
df = sql_sc.createDataFrame(data, ['name','age'])
df2 = sql_sc.createDataFrame(data2, ['name','age'])

# df is now built and shown.
df.show()

# select the name column
names = df.name


# select all
df.select('*').show()
#df2.show()

# select the first column and add 10 on to each row of second.
df.select(df.name, (df.age + 10).alias('age')).show()

# drop the second column
df.drop(df.age).show()

# makes df2 now!
df2.show()

#print distinct rows
df2.distinct().show()


#print the values that have have age greater than 2
df2.filter(df2.age > 2).show()

# sort by assending order.
df2.sort("age", ascending=False).show()



df3 = df2.groupBy(df2.name)
df3.agg({"*":"count"}).count()

print df3
#######

#from pyspark.sql.types import IntegerType

# user defined function 
#slen = sql_sc.udf.register(lambda s: len(s), IntegerType())

#d5 = df.select(slen(df.name).alias('slen'))

#d5.show() 