%spark.pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col


# load the dataset
df = spark.read.csv("/resources/results2.csv", inferSchema=True, header=True).toDF("date", "team", "rival", "h_score", "a_score", "tournament", "city", "country", "neutral")
df.dropDuplicates()
df.show(5)

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as f

#winner column
#df = df.withColumn('result', f.when(f.col('h_score') >= f.col('a_score'), "Win_Draw").otherwise("Lose"))
df = df.withColumn('result', f.when(f.col('h_score') > f.col('a_score'), "Win") 
                            .when(f.col('h_score') == f.col('a_score'), "Draw")
                            .otherwise(f.when(f.col('h_score') < f.col('a_score'), "Lose")))
df.show()
# drop the original data features column
df = df.drop('h_score', 'a_score', 'tournament', 'date', 'country', 'neutral')
df.show(30)

from pyspark.ml.feature import StringIndexer
# estimator
t_indexer = StringIndexer(inputCol="team", outputCol="teamIndex")
r_indexer = StringIndexer(inputCol="rival", outputCol="rivalIndex")
c_indexer = StringIndexer(inputCol="city", outputCol="cityIndex")
res_indexer = StringIndexer(inputCol="result", outputCol="resultIndex")

df = t_indexer.fit(df).transform(df)
df = r_indexer.fit(df).transform(df)
df = c_indexer.fit(df).transform(df)
df = res_indexer.fit(df).transform(df)
df.show(3)

# transformer
vector_assembler = VectorAssembler(inputCols=["teamIndex", "rivalIndex", "cityIndex"],outputCol="features")
df_temp = vector_assembler.transform(df)

# drop the original data features column
df = df_temp.drop('team', 'rival', 'city', 'teamIndex', 'rivalIndex', 'cityIndex', 'winner')
df.show(30)

# data splitting
(training,testing) = df.randomSplit([0.9,0.1])

from pyspark.ml.classification import DecisionTreeClassifier

# train our model using training data
dt = DecisionTreeClassifier(labelCol="resultIndex", featuresCol="features", maxBins=2015)

#paramGrid = ParamGridBuilder().addGrid(dt.maxBins, [49, 52, 55]).addGrid(dt.maxDepth, [4, 6, 8]).addGrid(rf.impurity, ["entropy", "gini"]).build()
#pipeline = Pipeline(stages=[labelIndexer, typeIndexer, assembler, dt])

model = dt.fit(training)
# test our model and make predictions using testing data
predictions = model.transform(testing)
predictions.select("prediction", "resultIndex").show(10)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(labelCol="resultIndex", predictionCol="prediction",metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))
print("Accuracy = %g " % accuracy)