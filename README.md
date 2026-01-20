from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType
from functools import reduce

spark = SparkSession.builder.appName("Call_DM").enableHiveSupport().getOrCreate()

hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
def path_exists(p):
    return hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p))

months = {
    '202501': 'jan', '202502': 'feb', '202503': 'mar',
    '202504': 'apr', '202505': 'may', '202506': 'jun',
    '202507': 'jul', '202508': 'aug', '202509': 'sep',
    '202510': 'oct', '202511': 'nov'
}

def safe_read(func, path, ym):
    try:
        if path_exists(path):
            df = func(path, ym)
            if df is not None and not df.rdd.isEmpty():
                return df
        else:
            print(path + " doesn't exist")
    except Exception as e:
        print(f"Error reading {path}: {e}")
    return None

def read_call(path, yearmonth):
    df = spark.read.option("header", True).csv(path)
    return df.select(
        lit(yearmonth).alias("yearmonth"),
        col("Customer No (CTI)").alias("user_id")
    )

dfs = []
for yearmonth, mnth in months.items():   # yearmonth=202501, mnth=jan
    p = f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"
    d = safe_read(read_call, p, yearmonth)
    if d is not None:
        dfs.append(d)

call_df = reduce(lambda d1, d2: d1.unionByName(d2), dfs) if dfs else spark.createDataFrame([], StructType([]))
target_db = "prd_tbl_uocolab_grp_nsen"
target_table = "call_monthly"   # choose your table name
full_table = f"{target_db}.{target_table}"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")

# First time: create table (Parquet) partitioned by yearmonth
# Next runs: append new partitions
if not spark.catalog.tableExists(full_table):
    (call_df
        .repartition(col("yearmonth"))               # optional, helps one partition per month
        .write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("yearmonth")
        .saveAsTable(full_table)
    )
else:
    # IMPORTANT: dynamic partitions
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    (call_df
        .repartition(col("yearmonth"))               # optional
        .write
        .format("parquet")
        .mode("append")
        .insertInto(full_table)                      # respects existing table partitions
    )
