from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException
from functools import reduce

# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("ChatNatureAnalytics") \
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
# READ CHAT
# --------------------------------------------------
def read_chat(path, part):
    df = spark.read.option("header", True).csv(path)
    return (
        df.filter(col("Pre/Post") == "Postlogin")
          .withColumn("mnth", lit(part))
          .select(
              col("REL ID").alias("user_id"),
              col("NumericId").alias("num_id"),
              col("mnth")
          )
    )

# --------------------------------------------------
# READ CHAT NATURE (FIX BAD HEADER)
# --------------------------------------------------
def read_chat_nature(path, part):
    df = spark.read.option("header", "false") \
        .option("inferSchema", "true") \
        .csv(path)

    # 3rd row is the real header
    new_header = df.limit(3).collect()[2]

    df_data = (
        df.rdd.zipWithIndex()
          .filter(lambda x: x[1] > 3)
          .map(lambda x: x[0])
          .toDF()
    )

    df_final = df_data.toDF(*new_header)

    return (
        df_final
        .withColumn("mnth", lit(part))
        .select(
            col("Numeric ID").alias("num_id"),
            col("Combine").alias("nature"),
            col("mnth")
        )
    )

# --------------------------------------------------
# SAFE READ
# --------------------------------------------------
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
chat_dfs = []
nature_dfs = []

for part, mnth in months.items():
    chat_path = f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv"
    nature_path = f"/user/2030435/CallCentreAnalystics/{mnth}_chat_nature.csv"

    df = safe_read(read_chat, part, chat_path)
    if df is not None:
        chat_dfs.append(df)

    df = safe_read(read_chat_nature, part, nature_path)
    if df is not None:
        nature_dfs.append(df)

# --------------------------------------------------
# UNION ALL MONTHS
# --------------------------------------------------
if chat_dfs:
    chat_df = reduce(lambda d1, d2: d1.unionByName(d2), chat_dfs)
else:
    chat_df = spark.createDataFrame([], StructType([]))

if nature_dfs:
    nature_df = reduce(lambda d1, d2: d1.unionByName(d2), nature_dfs)
else:
    nature_df = spark.createDataFrame([], StructType([]))

# --------------------------------------------------
# JOIN CHAT + NATURE
# --------------------------------------------------
final_df = chat_df.join(
    nature_df,
    on=["num_id", "mnth"],
    how="left"
)

# --------------------------------------------------
# AGGREGATE PER USER
# --------------------------------------------------
agg_df = final_df.groupBy("user_id").agg(
    count("*").alias("contact_count"),
    collect_set("nature").alias("natures"),
    collect_list("mnth").alias("months")
)

# --------------------------------------------------
# FILTER MULTI-CONTACT USERS
# --------------------------------------------------
final_df = agg_df.filter(col("contact_count") > 1)

# --------------------------------------------------
# ARRAY → STRING (CSV SAFE)
# --------------------------------------------------
final_df = final_df \
    .withColumn("natures", concat_ws("|", col("natures"))) \
    .withColumn("months", concat_ws("|", col("months")))

# --------------------------------------------------
# WRITE SINGLE CSV
# --------------------------------------------------
OUTPUT_DIR = "/user/2030435/CallCentreAnalystics/ChatNatureAgg"

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
                OUTPUT_DIR + "/ChatNatureAgg.csv"
            )
        )
