# ============================================================
# Call Monthly Raw Load -> Hive (Partitioned Append)
# Hadoop + Hive + Hue + Spark
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType
from functools import reduce
import re

# ---------------- SPARK SESSION ----------------
spark = SparkSession.builder.appName("Call_DM").enableHiveSupport().getOrCreate()

# ---------------- HDFS HELPERS ----------------
hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

def path_exists(p):
    return hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p))

# ---------------- CONFIG ----------------
months = {
    '202501': 'jan', '202502': 'feb', '202503': 'mar',
    '202504': 'apr', '202505': 'may', '202506': 'jun',
    '202507': 'jul', '202508': 'aug', '202509': 'sep',
    '202510': 'oct', '202511': 'nov'
}

BASE_PATH = "/user/2030435/CallCentreAnalystics"

TARGET_DB = "prd_tbl_uocolab_grp_nsen"
TARGET_TABLE = "call_monthly_raw"
FULL_TABLE = f"{TARGET_DB}.{TARGET_TABLE}"

# ---------------- COLUMN CLEANER (Hive/Spark safe) ----------------
def clean_col(c):
    # lower, trim
    c = (c or "").strip().lower()

    # replace ANY non-word chars (space, -, ., (), /, etc) with underscore
    c = re.sub(r"[^\w]+", "_", c)

    # collapse multiple underscores
    c = re.sub(r"_+", "_", c)

    # trim underscores
    c = c.strip("_")

    # Hive doesn't like empty column names
    if c == "":
        c = "col"

    # If starts with digit, prefix underscore
    if re.match(r"^\d", c):
        c = "_" + c

    return c

def make_unique(cols):
    seen = {}
    out = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")
    return out

# ---------------- SAFE READ WRAPPER ----------------
def safe_read(func, path, ym):
    try:
        if path_exists(path):
            df = func(path, ym)
            if df is not None and not df.rdd.isEmpty():
                return df
            else:
                print(f"{path} exists but empty")
        else:
            print(f"{path} doesn't exist")
    except Exception as e:
        print(f"Error reading {path}: {e}")
    return None

# ---------------- READ FULL CALL CSV + ADD YM + CLEAN COLS ----------------
def read_call(path, yearmonth):
    # read raw csv
    df = spark.read.option("header", True).csv(path)

    # add partition column
    df = df.withColumn("yearmonth", lit(yearmonth))

    # IMPORTANT: rename columns WITHOUT col("bad name") parsing
    new_cols = make_unique([clean_col(c) for c in df.columns])
    df = df.toDF(*new_cols)

    return df

# ---------------- BUILD MONTHLY UNION DF ----------------
dfs = []
for ym, mnth in months.items():
    path = f"{BASE_PATH}/{mnth}_call.csv"
    d = safe_read(read_call, path, ym)
    if d is not None:
        dfs.append(d)

if dfs:
    call_df = reduce(lambda d1, d2: d1.unionByName(d2, allowMissingColumns=True), dfs)
else:
    call_df = spark.createDataFrame([], StructType([]))

print("Final DF columns (sample):", call_df.columns[:30], "...")
callrows = call_df.count()
print("Total rows in call_df:", 0 if call_df.rdd.isEmpty() else Rrows)

call_df.printSchema()
call_df.show(5, truncate=False)

# ---------------- WRITE TO HIVE (PARTITIONED APPEND) ----------------
spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}")

# Dynamic partitions
spark.sql("SET hive.exec.dynamic.partition=true")
spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

# OPTIONAL: Idempotent load (drop these partitions before insert)
# Uncomment if you re-run months and DON'T want duplicates.
"""
ym_list = [r["yearmonth"] for r in call_df.select("yearmonth").distinct().collect()]
for ym in ym_list:
    spark.sql(f"ALTER TABLE {FULL_TABLE} DROP IF EXISTS PARTITION (yearmonth='{ym}')")
"""

if not spark.catalog.tableExists(FULL_TABLE):
    # First run: create table
    (call_df
        .repartition(col("yearmonth"))   # optional: helps write partition-wise
        .write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("yearmonth")
        .saveAsTable(FULL_TABLE)
    )
    print("Created table:", FULL_TABLE)
else:
    # Next runs: append partitions
    (call_df
        .repartition(col("yearmonth"))   # optional
        .write
        .format("parquet")
        .mode("append")
        .insertInto(FULL_TABLE)
    )
    print("Appended into table:", FULL_TABLE)

# ---------------- QUICK VALIDATION ----------------
spark.sql(f"SHOW PARTITIONS {FULL_TABLE}").show(200, truncate=False)
spark.sql(f"""
    SELECT yearmonth, COUNT(*) AS rows_cnt
    FROM {FULL_TABLE}
    GROUP BY yearmonth
    ORDER BY yearmonth
""").show(200, truncate=False)
