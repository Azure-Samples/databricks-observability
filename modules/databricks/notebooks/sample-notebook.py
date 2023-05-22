# Databricks notebook source
# MAGIC %md Run a command hitting the SQL Database metastore:

# COMMAND ----------

# MAGIC %sql show tables in samples.nyctaxi

# COMMAND ----------

# MAGIC %md Run a Spark job hitting the Azure Data Lake Storage Gen2 account:

# COMMAND ----------

(spark.table("samples.nyctaxi.trips")
.write
.mode("overwrite")
.option("path", spark.conf.get("storage_uri") + "/trips")
.saveAsTable("trips"))

# COMMAND ----------

# MAGIC %md Run an operation that causes the job to fail early about 50% of the time:

# COMMAND ----------

from datetime import datetime
invalidOp = 0 / (datetime.now().minute % 2) # causes division by zero at even-numbered minutes

# COMMAND ----------

# MAGIC %md Run an operation that takes a few minutes to complete:

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# COMMAND ----------

# Train/Test splitting
data = spark.table("trips")
data = data.withColumn('dayofweek', F.dayofweek(F.col('tpep_pickup_datetime')))
data = data.withColumn('hour', F.hour(F.col('tpep_pickup_datetime')))

(trainingData, testData) = data.randomSplit([0.7, 0.3], seed=1)

assembler = VectorAssembler(
    inputCols=['trip_distance', 'dayofweek', 'hour'],
    outputCol='features')
    
trainingData = assembler.setHandleInvalid("skip").transform(trainingData)
testData = assembler.setHandleInvalid("skip").transform(testData)

# COMMAND ----------

gbt = GBTRegressor(featuresCol='features', labelCol='fare_amount', maxDepth = 15, maxIter = 40, maxMemoryInMB = 2000)
m = gbt.fit(trainingData)
predictions = m.transform(testData)

evaluator = RegressionEvaluator(
    labelCol='fare_amount',  predictionCol="prediction", metricName="rmse")

rmse = evaluator.evaluate(predictions)
print("RMSE = %g" % rmse)