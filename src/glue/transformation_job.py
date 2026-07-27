import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, round as spark_round, date_format, to_date

# ---------------------------------------------------------
# Boilerplate Glue job setup
# ---------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------
# CONFIG — bucket and paths
# ---------------------------------------------------------
BUCKET_NAME = "yk-datalake-2026"
PROCESSED_PATH = f"s3://{BUCKET_NAME}/processed/"
CURATED_PATH = f"s3://{BUCKET_NAME}/curated/"

print(f"Reading cleansed data from: {PROCESSED_PATH}")

# ---------------------------------------------------------
# STEP 1: Read Parquet from processed/
# Why no schema/header options needed: Parquet is
# self-describing -- it already stores column names and
# types, unlike CSV.
# ---------------------------------------------------------
df = spark.read.parquet(PROCESSED_PATH)

input_count = df.count()
print(f"Input row count: {input_count}")

# ---------------------------------------------------------
# STEP 2: Ensure order_date is a proper date type
# Why: it may already be a string from the CSV origin, so
# we explicitly parse it before extracting the month.
# ---------------------------------------------------------
df = df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

# ---------------------------------------------------------
# STEP 3: Calculated column #1 -- total_prices
# ---------------------------------------------------------
df_transformed = df.withColumn(
    "total_price",
    spark_round(col("quantity") * col("unit_price"), 2)
)

# ---------------------------------------------------------
# STEP 4: Calculated column #2 -- order_month
# Why format "yyyy-MM" instead of just month number: keeps
# months from different years distinct (2025-01 vs 2026-01),
# which matters once this pipeline runs for more than a year.
# ---------------------------------------------------------
df_transformed = df_transformed.withColumn(
    "order_month",
    date_format(col("order_date"), "yyyy-MM")
)

# ---------------------------------------------------------
# STEP 5: Write output as Parquet to curated/
# ---------------------------------------------------------
df_transformed.write.mode("overwrite").parquet(CURATED_PATH)

output_count = df_transformed.count()
print(f"Transformed data written to: {CURATED_PATH}")
print(f"Output row count: {output_count}")
print("New columns added: total_price, order_month")

job.commit()
