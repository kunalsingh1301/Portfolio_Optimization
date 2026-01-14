from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException
from functools import reduce

# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("CallChatMultiContactAnalysis") \
    .getOrCreate()

# --------------------------------------------------
# MONTH CONFIG
# --------------------------------------------------
months = {
    '202501': 'jan', '202502': 'feb', '202503': 'mar',
    '202504': 'apr', '202505': 'may', '202506': 'jun',
    '202507': 'jul', '202508': 'aug', '202509': 'sep',
    '202510': 'oct', '202511': 'nov'
}

# --------------------------------------------------
# HDFS HELPERS
# --------------------------------------------------
hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

def path_exists(p):
    return hadoop_fs.exists(
        spark._jvm.org.apache.hadoop.fs.Path(p)
    )

# --------------------------------------------------
# READERS
# --------------------------------------------------
def read_call(path, part):
    df = spark.read.option("header", True).csv(path)
    return (
        df.filter(col("Customer No (CTI)").isNotNull())
          .select(
              col("Customer No (CTI)").alias("user_id"),
              lit("Call").alias("channel"),
              col("Primary Call Type").alias("primary_nature"),
              col("Secondary Call Type").alias("secondary_nature"),
              lit(part).alias("mnth")
          )
    )

def read_chat(path, part):
    df = spark.read.option("header", True).csv(path)
    return (
        df.filter(col("REL ID").isNotNull())
          .select(
              col("REL ID").alias("user_id"),
              lit("Chat").alias("channel"),
              lit(None).cast("string").alias("primary_nature"),
              lit(None).cast("string").alias("secondary_nature"),
              lit(part).alias("mnth")
          )
    )

def safe_read(func, part, path):
    try:
        if path_exists(path):
            df = func(path, part)
            if not df.rdd.isEmpty():
                return df
        else:
            print(f"Missing: {path}")
    except AnalysisException as e:
        print(f"AnalysisException: {e}")
    except Exception as e:
        print(f"Exception: {e}")
    return None

# --------------------------------------------------
# READ ALL MONTHS
# --------------------------------------------------
dfs = []

for part, mnth in months.items():

    call_path = f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"
    chat_path = f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv"

    call_df = safe_read(read_call, part, call_path)
    chat_df = safe_read(read_chat, part, chat_path)

    if call_df is not None:
        dfs.append(call_df)

    if chat_df is not None:
        dfs.append(chat_df)

# --------------------------------------------------
# UNION ALL
# --------------------------------------------------
if not dfs:
    raise ValueError("No data found")

combined_df = reduce(lambda d1, d2: d1.unionByName(d2), dfs).cache()

# --------------------------------------------------
# USER × CHANNEL AGGREGATION
# --------------------------------------------------
agg_df = combined_df.groupBy("user_id", "channel").agg(
    count("*").alias("contact_count"),
    collect_set("primary_nature").alias("primary_nature"),
    collect_set("secondary_nature").alias("secondary_nature")
)

# --------------------------------------------------
# FILTER MULTI-CONTACT USERS
# --------------------------------------------------
final_df = agg_df.filter(col("contact_count") > 1)

# --------------------------------------------------
# ARRAY → STRING (CSV SAFE)
# --------------------------------------------------
final_df = (
    final_df
    .withColumn("primary_nature", concat_ws("|", col("primary_nature")))
    .withColumn("secondary_nature", concat_ws("|", col("secondary_nature")))
)

# --------------------------------------------------
# OPTIONAL CLEANUP (REMOVE EMPTY)
# --------------------------------------------------
final_df = final_df.filter(
    (trim(col("user_id")) != "") &
    (trim(col("channel")) != "")
)

# --------------------------------------------------
# WRITE OUTPUT
# --------------------------------------------------
OUTPUT_DIR = "/user/2030435/CallCentreAnalystics/CallChatNature"

final_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(OUTPUT_DIR)

# --------------------------------------------------
# RENAME part FILE
# --------------------------------------------------
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

path = spark._jvm.org.apache.hadoop.fs.Path(OUTPUT_DIR)

for f in fs.listStatus(path):
    name = f.getPath().getName()
    if name.startswith("part-") and name.endswith(".csv"):
        fs.rename(
            f.getPath(),
            spark._jvm.org.apache.hadoop.fs.Path(
                OUTPUT_DIR + "/CallChatMultiContact.csv"
            )
        )
