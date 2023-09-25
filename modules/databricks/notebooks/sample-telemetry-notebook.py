# Databricks notebook source
# exports notebook_name, tracer, logger, meter

# COMMAND ----------

# MAGIC %run ./telemetry-helper

# COMMAND ----------

import time

save_duration_metric = meter.create_histogram(
    name="save_duration",
    description="Duration in seconds of the save operation",
    unit="s"
)

with tracer.start_as_current_span("process trips"):
    # The use of MDC allows us to correlate (Java) executor logs given a
    # known Python trace context. The correlation needs to be
    # established manually via the use of the `mdc.pyspark*` custom
    # dimensions.
    # https://spark.apache.org/docs/latest/configuration.html#configuring-logging
    spark.sparkContext.setLocalProperty("mdc.pyspark_trace_id", trace.format_trace_id(trace.get_current_span().get_span_context().trace_id))
    spark.sparkContext.setLocalProperty("mdc.pyspark_span_id", trace.format_span_id(trace.get_current_span().get_span_context().span_id))

    with tracer.start_as_current_span("write trips table"):

        logger.info("Saving data to table %s", "trips2")

        t0 = time.time()

        (spark.table("samples.nyctaxi.trips")
        .write
        .mode("overwrite")
        .option("path", spark.conf.get("storage_uri") + "/trips2")
        .saveAsTable("trips2"))

        save_duration_metric.record(time.time() - t0)

    with tracer.start_as_current_span("count trips table"):

        trips_saved = spark.table("trips2").count()

        logger.info("%d trips completed", trips_saved)