# Databricks notebook source
# MAGIC %run ./telemetry-helper

# COMMAND ----------

# Define a metric to track average value
avg_value_metric = meter.create_histogram(
    name="avg_value",
    description="Average value",
    unit="1"
)

# COMMAND ----------

from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.functions import *

# Create a query listener to track metrics
# See https://www.databricks.com/blog/2022/05/27/how-to-monitor-streaming-queries-in-pyspark.html

# Define listener
class ValueTrackingListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"'{event.name}' [{event.id}] got started!")
    def onQueryProgress(self, event):
        row = event.progress.observedMetrics.get("metric")
        avg_value_metric.record(row.avg_value)
        print(f"Recorded meter {meter.name} metric {avg_value_metric.name} value {row.avg_value}")
    def onQueryTerminated(self, event):
        print(f"{event.id} got terminated!")

# Add listener
listener = ValueTrackingListener()
spark.streams.addListener(listener)

# COMMAND ----------

streaming_df = (spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 100)
    .load())

observed_streaming_df = streaming_df.observe(
    "metric",
    count(lit(1)).alias("cnt"),  # number of processed rows
    avg(col("value")).alias("avg_value"))  # average of row values

# COMMAND ----------

# Streaming query
query = (observed_streaming_df
    .writeStream
    .format("console")
    .queryName("Rate query")
    .start())

# COMMAND ----------

import time
time.sleep(120)
query.stop()
spark.streams.removeListener(listener)