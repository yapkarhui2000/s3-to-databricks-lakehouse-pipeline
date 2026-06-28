# Databricks notebook source

def customer_bronze():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/path/to/schema/metadata")
        .option("header", "true")
        .load("s3://data-warehouse-project.../source_crm/"))
