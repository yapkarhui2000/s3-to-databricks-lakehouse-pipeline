# s3-to-databricks-lakehouse-pipeline

## Project Overview
This project demonstrates the design and implementation of an end-to-end cloud data engineering pipeline. The goal was to build a scalable infrastructure that takes raw, unoptimized data, processes it into a clean and structured format, and makes it available for downstream business intelligence.

The data pipeline consists of three parts:

* **1. Data Sourcing & Staging (AWS S3):** Acquired raw CSV datasets from an open-source GitHub repository and staged them within an Amazon S3 bucket, which serves as the immutable data lake landing zone.
* **2. Data Ingestion & Processing (Databricks):** Integrated Databricks with AWS S3 to ingest the raw data. Executed robust ETL (Extract, Transform, Load) workloads using PySpark to handle data cleansing, schema enforcement, and structural transformations.
* **3. BI Availability (Power BI):** Exposed the finalized, optimized data tables directly to Power BI, validating that the pipeline successfully delivers clean, high-performance datasets for reporting.

## Data Pipeline Overview
<img width="1245" height="416" alt="Data_Pipeline_Overview" src="https://github.com/user-attachments/assets/fa19fcd7-e498-4f56-a7d5-6633b0973b53" />

The pipeline and runtimes were designed to host on Cloud environments (AWS and DataBricks). This approach ensures a scalable and cost-effective solution for processing data in a timely manner.
