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

##  Data Architecture

This project follows the **Medallion Architecture** approach.

### Architecture Layers

####  Bronze Layer

* Raw data ingestion from source systems
* Minimal transformations
* Preserves original source data

####  Silver Layer

* Data cleansing and validation
* Standardization and enrichment
* Business rule implementation

####  Gold Layer

* Business-ready dimensional models
* Optimized for reporting and analytics
* Supports dashboards and decision-making

##  Architecture Diagram

<img width="1213" height="795" alt="Data_Architecture_Diagram" src="https://github.com/user-attachments/assets/b15c0c18-7af9-4176-b18e-14750e23fa2b" />

---
##  TechStacks

* **Cloud Storage:** Amazon S3
* **Compute & Processing:** Databricks (Apache Spark / PySpark)
* **Language:** Python / SQL
* **Data Consumer:** Microsoft Power BI

##  Requirements

To replicate or run this pipeline, the following environment setups and accounts are required:

### 1. Cloud Infrastructure & Access
* **AWS Account:** Active access to AWS with permissions to create and manage Amazon S3 buckets.
* **IAM Security:** An AWS IAM User or Role configured with programmatic access (`Read/Write` policies) to allow Databricks to authenticate and mount/access the S3 storage.
* **Databricks Workspace:** A Databricks workspace (Community Edition or Enterprise) hosted on AWS.

### 2. Compute Runtime (Databricks Cluster)
* **Databricks Runtime:** Recommended version `13.3 LTS` or higher (includes Apache Spark 3.4.x, Scala 2.12).
* **Worker Type:** Single Node cluster configuration is sufficient for handling these CSV workloads.

### 3. Desktop Software & Libraries
* **Python 3.x:** For reviewing or modifying local PySpark script files.
* **Microsoft Power BI Desktop:** Required to connect to the Databricks cluster endpoint using personal access tokens (PAT) or Azure/AWS credentials for final reporting.
