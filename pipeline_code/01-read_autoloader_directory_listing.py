# Databricks notebook source
import dlt
import pyspark.sql.functions as F

@dlt.table(
    name="customers_with_pii",
    comment="Data with PII intact ingested from JSON in landing"
)
def customers_with_pii():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.fetchParallelism", 32)
        .load("{}/customers".format(spark.conf.get("ingestion_folder")))
        .select(
            "*",
            "_metadata.*",
            F.current_timestamp().alias('processing_time')
        )
    )
    return df