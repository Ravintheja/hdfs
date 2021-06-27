#Model to predict winner
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pandas

Sc = SparkContext()
sqlContext = SQLContext(sc)

#Load data
company_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/Spark_data/results.csv')
#show 1
company_df.take(1)

company_df.cache()
company_df.printSchema()