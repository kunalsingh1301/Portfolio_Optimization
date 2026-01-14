from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException
from functools import reduce

# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("CallChatNatureAnalytics") \
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
    return hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p))

# --------------------------------------------------
# SAFE READ
# --------------------------------------------------
def safe_read(func, path, part):
    try:
        if path_exists(path):
            df = func(path, part)
            if df is not None and not df.rdd.isEmpty():
                return df
        else:
            print(f"Missing: {path}")
    except AnalysisException as e:
        print(f"AnalysisException: {e}")
    except Exception as e:
        print(f"Exception: {e}")
    return None

# --------------------------------------------------
# READ CALL
# --------------------------------------------------
def read_call(path, part):
    df = spark.read.option("header", True).csv(path)
    return (
        df.filter(col("Customer No (CTI)").isNotNull())
          .withColumn("mnth", lit(part))
          .withColumn("channel", lit("Call"))
          .select(
              col("Customer No (CTI)").alias("user_id"),
              col("Primary Call Type").alias("primary_nature"),
              col("Secondary Call Type").alias("secondary_nature"),
              "channel",
              "mnth"
          )
    )

# --------------------------------------------------
# READ CHAT
# --------------------------------------------------
def read_chat(path, part):
    df = spark.read.option("header", True).csv(path)
    return (
        df.filter(col("Pre/Post") == "Postlogin")
          .withColumn("mnth", lit(part))
          .withColumn("channel", lit("Chat"))
          .select(
              col("REL ID").alias("user_id"),
              col("NumericId").alias("num_id"),
              "channel",
              "mnth"
          )
    )

# --------------------------------------------------
# READ CHAT NATURE
# --------------------------------------------------
def read_chat_nature(path, part):
    df = spark.read.option("header", "false") \
                   .option("inferSchema", "true") \
                   .csv(path)
    # assume 3rd row has headers
    new_header = df.limit(3).collect()[2]
    df_data = df.rdd.zipWithIndex().filter(lambda x: x[1] > 3).map(lambda x: x[0]).toDF()
    df_final = df_data.toDF(*new_header)
    return (
        df_final
        .withColumn("mnth", lit(part))
        .withColumn("channel", lit("Chat"))
        .select(
            col("Numeric ID").alias("num_id"),
            col("Combine").alias("chat_nature"),
            "channel",
            "mnth"
        )
    )

# --------------------------------------------------
# READ AND UNION ALL MONTHS
# --------------------------------------------------
call_dfs, chat_dfs, chat_nature_dfs = [], [], []

for part, mnth in months.items():
    call_path = f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"
    chat_path = f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv"
    chat_nature_path = f"/user/2030435/CallCentreAnalystics/{mnth}_chat_nature.csv"

    df = safe_read(read_call, call_path, part)
    if df is not None: call_dfs.append(df)

    df = safe_read(read_chat, chat_path, part)
    if df is not None: chat_dfs.append(df)

    df = safe_read(read_chat_nature, chat_nature_path, part)
    if df is not None: chat_nature_dfs.append(df)

# UNION
call_df = reduce(lambda d1, d2: d1.unionByName(d2), call_dfs) if call_dfs else spark.createDataFrame([], StructType([]))
chat_df = reduce(lambda d1, d2: d1.unionByName(d2), chat_dfs) if chat_dfs else spark.createDataFrame([], StructType([]))
chat_nature_df = reduce(lambda d1, d2: d1.unionByName(d2), chat_nature_dfs) if chat_nature_dfs else spark.createDataFrame([], StructType([]))

# JOIN CHAT + NATURE
chat_df = chat_df.join(chat_nature_df, on=["num_id","mnth","channel"], how="left")

# DROP num_id now
chat_df = chat_df.drop("num_id")

# --------------------------------------------------
# COMBINE CALL + CHAT
# --------------------------------------------------
combined_df = call_df.unionByName(chat_df, allowMissingColumns=True)

# --------------------------------------------------
# MAKE SURE ALL NATURE COLUMNS EXIST
# --------------------------------------------------
for c in ["primary_nature", "secondary_nature", "chat_nature"]:
    if c not in combined_df.columns:
        combined_df = combined_df.withColumn(c, array().cast("array<string>"))

# --------------------------------------------------
# AGGREGATE PER USER + CHANNEL
# --------------------------------------------------
agg_df = combined_df.groupBy("user_id","channel").agg(
    count("*").alias("contact_count"),
    collect_set("primary_nature").alias("primary_nature"),
    collect_set("secondary_nature").alias("secondary_nature"),
    collect_set("chat_nature").alias("chat_nature")
)

# --------------------------------------------------
# CONVERT ARRAYS → STRING
# --------------------------------------------------
agg_df = agg_df \
    .withColumn("primary_nature", concat_ws("|", col("primary_nature"))) \
    .withColumn("secondary_nature", concat_ws("|", col("secondary_nature"))) \
    .withColumn("chat_nature", concat_ws("|", col("chat_nature")))

# --------------------------------------------------
# WRITE RESULT
# --------------------------------------------------
OUTPUT_DIR = "/user/2030435/CallCentreAnalystics/CombinedCallChatNature"

agg_df.coalesce(1).write.mode("overwrite").option("header", True).csv(OUTPUT_DIR)

# --------------------------------------------------
# RENAME PART FILE
# --------------------------------------------------
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
path = spark._jvm.org.apache.hadoop.fs.Path(OUTPUT_DIR)
for f in fs.listStatus(path):
    name = f.getPath().getName()
    if name.startswith("part-") and name.endswith(".csv"):
        fs.rename(
            f.getPath(),
            spark._jvm.org.apache.hadoop.fs.Path(OUTPUT_DIR + "/CombinedCallChatNatureDF.csv")
        )
