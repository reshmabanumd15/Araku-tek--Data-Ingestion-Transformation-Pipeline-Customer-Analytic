import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME","S3_BUCKET","RAW_PREFIX","PROCESSED_PREFIX","RUN_DATE"])
bucket = args["S3_BUCKET"]
raw = args["RAW_PREFIX"]
processed = args["PROCESSED_PREFIX"]
run_date = args["RUN_DATE"]  # YYYY-MM-DD

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

def process(domain, reader):
    src = f"s3://{bucket}/{raw}/{domain}/ingest_date={run_date}/"
    df = reader(src)
    df = df.withColumn("ingest_date", F.lit(run_date))
    dst = f"s3://{bucket}/{processed}/{domain}/"
    (df.repartition(1).write.mode("overwrite").partitionBy("ingest_date").parquet(dst))

def read_customers(path):
    df = spark.read.option("header", True).csv(path)
    return (df
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("signup_date", F.to_date("signup_date"))
        .withColumn("lifetime_value", F.col("lifetime_value").cast("double"))
    )

def read_transactions(path):
    df = spark.read.option("header", True).csv(path)
    return (df
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("amount", F.col("amount").cast("double"))
    )

process("customer", read_customers)
process("transactions", read_transactions)
job.commit()
