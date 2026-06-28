from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Bronze layer: Ingest customer data from S3 using Auto Loader
@dp.table(
    comment="Raw customer information ingested from S3 CRM system"
)
def customer_bronze():
    """
    Ingest customer data from S3 using Auto Loader.
    Filters only cust_info.csv from the S3 folder.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "cust_info.csv")
        .load("s3://data-warehouse-project-139279686689-ap-southeast-2-an/source_crm/")
    )


# Bronze layer: Ingest product data from S3 using Auto Loader
@dp.table(
    comment="Raw product information ingested from S3 CRM system"
)
def product_bronze():
    """
    Ingest product data from S3 using Auto Loader.
    Filters only prd_info.csv from the S3 folder.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "prd_info.csv")
        .load("s3://data-warehouse-project-139279686689-ap-southeast-2-an/source_crm/")
    )


# Bronze layer: Ingest sales data from S3 using Auto Loader
@dp.table(
    comment="Raw sales details ingested from S3 CRM system"
)
def sales_bronze():
    """
    Ingest sales data from S3 using Auto Loader.
    Filters only sales_details.csv from the S3 folder.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "sales_details.csv")
        .load("s3://data-warehouse-project-139279686689-ap-southeast-2-an/source_crm/")
    )

# Bronze layer: Ingest cust_AZ12 data from S3 using Auto Loader
@dp.table(
    comment="Raw cust_AZ12 ingested from S3 ERP system"
)
def cust_az12():
    """
    Ingest cust_AZ12 from S3 using Auto Loader.
    Filters only cust_AZ12.csv from the S3 folder.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "CUST_AZ12.csv")
        .load("s3://data-warehouse-project-139279686689-ap-southeast-2-an/source_erp/")
    )

# Bronze layer: Ingest LOC_A101 data from S3 using Auto Loader
@dp.table(
    comment="Raw LOC_A101 ingested from S3 ERP system"
)
def LOC_A101():
    """
    Ingest LOC_A101 from S3 using Auto Loader.
    Filters only LOC_A101.csv from the S3 folder.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "LOC_A101.csv")
        .load("s3://data-warehouse-project-139279686689-ap-southeast-2-an/source_erp/")
    )

# Bronze layer: Ingest PX_CAT_G1V2 data from S3 using Auto Loader
@dp.table(
    comment="Raw PX_CAT_G1V2 ingested from S3 ERP system"
)
def PX_CAT_G1V2():
    """
    Ingest PX_CAT_G1V2 from S3 using Auto Loader.
    Filters only PX_CAT_G1V2.csv from the S3 folder.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "PX_CAT_G1V2.csv")
        .load("s3://data-warehouse-project-139279686689-ap-southeast-2-an/source_erp/")
    )
