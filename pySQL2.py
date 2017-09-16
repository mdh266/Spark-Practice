from pyspark import SparkContext
from pyspark.sql import SQLContext
import sqlite3
import pandas as pd

sc = SparkContext('local','pySQL2.py')
sql_sc = SQLContext(sc)


conn = sqlite3.connect('SALARIES.db')
pd_df = pd.read_sql_query("SELECT * FROM Employee", conn)
conn.close()

# describe how to make the 
df = sql_sc.createDataFrame(pd_df)

# save it to memory after reading it in first time.
df.cache()

df.printSchema()

# would normally re-read it from disk again if originally did it
# but now should be 
# stored in memory.
print "rows = " + str(df.count())