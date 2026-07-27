import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, trim

# ---------------------------------------------------------
# Boilerplate Glue job setup
# Why: Glue jobs need these context objects to talk to
# Spark, the Glue Catalog, and to track job bookmarks.
# ---------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------
# CONFIG — bucket and paths
# Why hardcode here instead of parameterizing: keeps this
# beginner-friendly and console-driven, matching the
# project's "no Infrastructure as Code" requirement.
# ---------------------------------------------------------
BUCKET_NAME = "yk-datalake-2026"
RAW_PATH = f"s3://{BUCKET_NAME}/raw/"
PROCESSED_PATH = f"s3://{BUCKET_NAME}/processed/"

# Mandatory fields per problem statement -- rows missing
# any of these should be dropped
MANDATORY_FIELDS = ["branch", "product", "quantity", "unit_price"]

print(f"Reading raw data from: {RAW_PATH}")

# ---------------------------------------------------------
# STEP 1: Read CSV from raw/
# Why header=True and inferSchema=True: our source files
# have a header row, and letting Spark infer types (int,
# double, string) avoids manually declaring a schema.
# ---------------------------------------------------------
df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH)

initial_count = df.count()
print(f"Initial row count: {initial_count}")

# ---------------------------------------------------------
# STEP 2: Drop exact duplicate rows
# Why dropDuplicates() with no args: it compares ALL
# columns, so only true exact-match duplicates are removed
# -- not just duplicate order_ids, which could be legitimate
# re-orders.
# ---------------------------------------------------------
df_no_dupes = df.dropDuplicates()

after_dedup_count = df_no_dupes.count()
print(f"Rows after removing duplicates: {after_dedup_count} "
      f"(removed {initial_count - after_dedup_count})")

# ---------------------------------------------------------
# STEP 3: Drop rows missing mandatory fields
# Why trim() before the null check: a field containing only
# whitespace ("   ") isn't caught by isNull(), so we clean
# whitespace-only values into true nulls first, then filter.
# ---------------------------------------------------------
df_clean = df_no_dupes
for field in MANDATORY_FIELDS:
    df_clean = df_clean.withColumn(
        field,
        trim(col(field).cast("string"))
    )
    df_clean = df_clean.filter(
        (col(field).isNotNull()) & (col(field) != "")
    )

final_count = df_clean.count()
print(f"Rows after removing missing-mandatory-field rows: {final_count} "
      f"(removed {after_dedup_count - final_count})")

# ---------------------------------------------------------
# STEP 4: Re-cast numeric fields back to proper types
# Why: the trim() step above cast quantity/unit_price to
# string for the null-check; we cast them back so Parquet
# stores them as numeric types, not strings.
# ---------------------------------------------------------
df_final = df_clean \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("unit_price", col("unit_price").cast("double"))

# ---------------------------------------------------------
# STEP 5: Write output as Parquet to processed/
# Why mode("overwrite"): re-running this job during testing
# shouldn't keep appending duplicate data on top of itself.
# ---------------------------------------------------------
df_final.write.mode("overwrite").parquet(PROCESSED_PATH)

print(f"Cleansed data written to: {PROCESSED_PATH}")
print(f"Final record count: {final_count}")

job.commit()
