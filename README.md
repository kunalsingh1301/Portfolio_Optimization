from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException
from functools import reduce

# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("CallCentreAnalytics") \
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
          .withColumn("mnth", lit(part))
          .select(
              col("Customer No (CTI)").alias("user_id"),
              col("Primary Call Type"),
              col("Secondary Call Type")
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
    df = safe_read(read_call, part, call_path)
    if df is not None:
        dfs.append(df)

# --------------------------------------------------
# UNION ALL DATA
# --------------------------------------------------
if dfs:
    combined_df = reduce(lambda d1, d2: d1.unionByName(d2), dfs)
else:
    combined_df = spark.createDataFrame([], StructType([]))

# --------------------------------------------------
# AGGREGATION
# --------------------------------------------------
agg_df = combined_df.groupBy("user_id").agg(
    count("*").alias("contact_count"),
    collect_set("Primary Call Type").alias("Prim_call_type"),
    collect_list("Secondary Call Type").alias("Sec_call_type")
)

# --------------------------------------------------
# FILTER MULTIPLE CONTACT USERS
# --------------------------------------------------
final_df = agg_df.filter(col("contact_count") > 1)

# --------------------------------------------------
# ARRAY → STRING (CSV SAFE)
# --------------------------------------------------
final_df = final_df \
    .withColumn("Prim_call_type", concat_ws("|", col("Prim_call_type"))) \
    .withColumn("Sec_call_type", concat_ws("|", col("Sec_call_type")))

final_df = final_df.filter(
  col("Prim_call_type").isNotNull() &
  col("Sec_call_type").isNotNull() &
  (trim(col("Prim_call_type")) !="")&
  (trim(col("Sec_call_type")) !="")
)

# --------------------------------------------------
# WRITE SINGLE CSV FILE
# --------------------------------------------------
OUTPUT_DIR = "/user/2030435/CallCentreAnalystics/CallNature"

final_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(OUTPUT_DIR)

# --------------------------------------------------
# RENAME part file → FIXED NAME
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
                OUTPUT_DIR + "/CallNatureDF.csv"
            )
        )
"To Understand whether customer had mulitple contacts over past few months or within a single months across different channel. If yes, what are the call / livechat nature ? (call we have nature )
Agent Chat - look up the the interaction from Daily Chat Nature ( column F to get the nature ) 
Chat - will not hav ethe nature "
