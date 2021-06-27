#Spark Script 01 - Countries that have hosted more than 1 tournament
print('====== Script 01 ======')

ftball_res = spark.read.csv("/Spark_data/results.csv", header = "true")

one = ftball_res.groupBy("tournament", "country").count()
two = one.groupBy("country").count()
three = two.filter(two["count"] > 1)
four = three.count()
five = ftball_res.select("country").distinct().count()
final = (float(four)/float(five) * 100).show()