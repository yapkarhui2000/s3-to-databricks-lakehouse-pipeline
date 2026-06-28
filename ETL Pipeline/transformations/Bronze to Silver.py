# Databricks notebook source
# DBTITLE 1,Imports and Setup
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Silver: Customer Info
@dp.materialized_view(
    name="datawarehouse_project.silver.cust_info",
    comment="Transformed customer information with normalized gender and deduplication",
)
def cust_info():
    # Read from bronze table
    df_bronze = spark.read.table("customer_bronze")
    
    # Filter out NULL cst_id
    df_filtered = df_bronze.filter(F.col("cst_id").isNotNull())
    
    # Define window spec for deduplication (keep most recent record per customer)
    window_spec = Window.partitionBy("cst_id").orderBy(F.col("cst_create_date").desc())
    
    # Add row number and keep only the most recent record
    df_deduplicated = df_filtered.withColumn(
        "flag_last", F.row_number().over(window_spec)
    ).filter(F.col("flag_last") == 1)
    
    # Apply transformations
    return df_deduplicated.select(
        F.col("cst_id"),
        F.col("cst_key"),
        F.trim(F.col("cst_firstname")).alias("cst_firstname"),
        F.trim(F.col("cst_lastname")).alias("cst_lastname"),
        F.when(
            F.upper(F.trim(F.col("cst_marital_status"))) == 'M', 'Married'
        ).when(
            F.upper(F.trim(F.col("cst_marital_status"))) == 'S', 'Single'
        ).otherwise('n/a').alias("cst_marital_status"),
        F.when(
            F.upper(F.trim(F.col("cst_gndr"))) == 'F', 'Female'
        ).when(
            F.upper(F.trim(F.col("cst_gndr"))) == 'M', 'Male'
        ).otherwise('n/a').alias("cst_gndr"),
        F.col("cst_create_date")
    )

# COMMAND ----------

# DBTITLE 1,Silver: Product Info

@dp.materialized_view(
    name="datawarehouse_project.silver.prd_info",
    comment="Transformed product information with extracted category ID",
)
def prd_info():
    # Read from bronze table
    df_bronze = spark.read.table("product_bronze")

    # Apply transformations using .select()
    return df_bronze.select(
        F.col("prd_id"),
        # Extract category ID (Get the first bit of the product key to join table)
        F.regexp_replace(F.substring(F.col("prd_key"), 1, 5), "-", "_").alias(
            "cate_id"
        ),
        # Extract Product Key (Get the last bit of the product key from position 7 onwards)
        F.substring(F.col("prd_key"), 7, F.length(F.col("prd_key"))).alias("prd_key"),
        F.col("prd_nm"),
        F.col("prd_cost"),
        F.col("prd_line"),
        F.col("prd_start_dt"),
        F.col("prd_end_dt"),
    )

# COMMAND ----------

# DBTITLE 1,Silver: Sales Details
@dp.materialized_view(
    name="datawarehouse_project.silver.sales_details",
    comment="Cleaned sales details with validated dates and recalculated amounts"
)
def sales_details():
    df_bronze = spark.read.table("sales_bronze")
    return df_bronze.select(
        F.col("sls_ord_num"),
        F.col("sls_prd_key"),
        F.col("sls_cust_id"),
        F.when(
            (F.col("sls_order_dt") == 0) | (F.length(F.col("sls_order_dt").cast("string")) != 8),
            F.lit(None)
        ).otherwise(
            F.to_date(F.col("sls_order_dt").cast("string"), "yyyyMMdd")
        ).alias("sls_order_dt"),
        F.when(
            (F.col("sls_ship_dt") == 0) | (F.length(F.col("sls_ship_dt").cast("string")) != 8),
            F.lit(None)
        ).otherwise(
            F.to_date(F.col("sls_ship_dt").cast("string"), "yyyyMMdd")
        ).alias("sls_ship_dt"),
        F.when(
            (F.col("sls_due_dt") == 0) | (F.length(F.col("sls_due_dt").cast("string")) != 8),
            F.lit(None)
        ).otherwise(
            F.to_date(F.col("sls_due_dt").cast("string"), "yyyyMMdd")
        ).alias("sls_due_dt"),
        F.when(
            (F.col("sls_sales").isNull()) | 
            (F.col("sls_sales") <= 0) | 
            (F.col("sls_sales") != F.col("sls_quantity") * F.abs(F.col("sls_price"))),
            F.col("sls_quantity") * F.abs(F.col("sls_price"))
        ).otherwise(
            F.col("sls_sales")
        ).alias("sls_sales"),
        F.col("sls_quantity"),
        F.when(
            (F.col("sls_price").isNull()) | (F.col("sls_price") <= 0),
            F.col("sls_sales") / F.when(F.col("sls_quantity") == 0, F.lit(None)).otherwise(F.col("sls_quantity"))
        ).otherwise(
            F.col("sls_price")
        ).alias("sls_price")
    )

# COMMAND ----------

# DBTITLE 1,Silver: Customer AZ12
@dp.materialized_view(
    name="datawarehouse_project.silver.cust_az12",
    comment="Normalized customer data from ERP system az12"
)
def cust_az12():
    # Read from bronze table
    df_bronze = spark.read.table("cust_az12")
    
    # Apply transformations using .select()
    return df_bronze.select(
        # Remove 'NAS' prefix from cid if present
        F.when(
            F.col("cid").like("NAS%"),
            F.substring(F.col("cid"), 4, F.length(F.col("cid")))
        ).otherwise(
            F.col("cid")
        ).alias("cid"),
        
        # Set future birthdates to NULL
        F.when(
            F.col("bdate") > F.current_date(),
            F.lit(None)
        ).otherwise(
            F.col("bdate")
        ).alias("bdate"),
        
        # Normalize gender values and handle unknown cases
        F.when(
            F.upper(F.trim(F.col("gen"))).isin(["F", "FEMALE"]),
            "Female"
        ).when(
            F.upper(F.trim(F.col("gen"))).isin(["M", "MALE"]),
            "Male"
        ).otherwise(
            "n/a"
        ).alias("gen")
    )

# COMMAND ----------

# DBTITLE 1,Silver: Location A101
@dp.materialized_view(
    name="datawarehouse_project.silver.loc_a101",
    comment="Normalized location data from ERP system a101"
)
def loc_a101():
    # Read from bronze table
    df_bronze = spark.read.table("loc_a101")
    
    # Apply transformations using .select()
    return df_bronze.select(
        # Remove hyphens from cid
        F.regexp_replace(F.col("cid"), "-", "").alias("cid"),
        
        # Normalize and handle missing or blank country codes
        F.when(
            F.trim(F.col("cntry")) == "DE",
            "Germany"
        ).when(
            F.trim(F.col("cntry")).isin(["US", "USA"]),
            "United States"
        ).when(
            (F.trim(F.col("cntry")) == "") | (F.col("cntry").isNull()),
            "n/a"
        ).otherwise(
            F.trim(F.col("cntry"))
        ).alias("cntry")
    )

# COMMAND ----------

# DBTITLE 1,Silver: Product Category
@dp.materialized_view(
    name="datawarehouse_project.silver.px_cat_g_1_v_2",
    comment="Product category data from ERP system"
)
def px_cat_g_1_v_2():
    # Read from bronze table
    df_bronze = spark.read.table("px_cat_g1v2")
    
    # Drop _rescued_data column if it exists
    return df_bronze.drop("_rescued_data")
