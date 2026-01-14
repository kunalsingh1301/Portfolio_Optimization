from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException
from functools import reduce

# ==================================================
# SPARK SESSION
# ==================================================
spark = SparkSession.builder \
    .appName("UnifiedCallChatNature") \
    .getOrCreate()

# ==================================================
# MONTH CONFIG
# ==================================================
months = {
    '202501': 'jan', '202502': 'feb', '202503': 'mar',
    '202504': 'apr', '202505': 'may', '202506': 'jun',
    '202507': 'jul', '202508': 'aug', '202509': 'sep',
    '202510': 'oct', '202511': 'nov'
}

# ==================================================
# HDFS HELPERS
# ==================================================
hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

def path_exists(p):
    return hadoop_fs.exists(
        spark._jvm.org.apache.hadoop.fs.Path(p)
    )

def safe_read(func, part, path):
    try:
        if path_exists(path):
            df = func(path, part)
            if not df.rdd.isEmpty():
                return df
        else:
            print(f"Missing: {path}")
    except Exception as e:
        print(e)
    return None

# ==================================================
# READ CALL
# ==================================================
def read_call(path, part):
    df = spark.read.option("header", True).csv(path)
    return (
        df.filter(col("Customer No (CTI)").isNotNull())
          .select(
              col("Customer No (CTI)").alias("user_id"),
              col("Primary Call Type").alias("primary_nature"),
              col("Secondary Call Type").alias("secondary_nature")
          )
          .withColumn("channel", lit("CALL"))
    )

# ==================================================
# READ CHAT
# ==================================================
def read_chat(path, part):
    df = spark.read.option("header", True).csv(path)
    return (
        df.filter(col("Pre/Post") == "Postlogin")
          .select(
              col("REL ID").alias("user_id"),
              col("NumericId").alias("num_id")
          )
          .withColumn("channel", lit("CHAT"))
          .withColumn("mnth", lit(part))
    )

# ==================================================
# READ CHAT NATURE (FIX HEADER)
# ==================================================
def read_chat_nature(path, part):
    df = spark.read.option("header", "false").csv(path)
    header = df.limit(3).collect()[2]

    df_data = (
        df.rdd.zipWithIndex()
          .filter(lambda x: x[1] > 3)
          .map(lambda x: x[0])
          .toDF()
    )

    df_final = df_data.toDF(*header)

    return (
        df_final
        .select(
            col("Numeric ID").alias("num_id"),
            col("Combine").alias("chat_nature")
        )
        .withColumn("mnth", lit(part))
    )

# ==================================================
# READ ALL MONTHS
# ==================================================
call_dfs, chat_dfs, chat_nat_dfs = [], [], []

for part, mnth in months.items():
    call_dfs.append(
        safe_read(read_call, part, f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv")
    )
    chat_dfs.append(
        safe_read(read_chat, part, f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv")
    )
    chat_nat_dfs.append(
        safe_read(read_chat_nature, part, f"/user/2030435/CallCentreAnalystics/{mnth}_chat_nature.csv")
    )

call_df = reduce(lambda a,b: a.unionByName(b), filter(None, call_dfs))
chat_df = reduce(lambda a,b: a.unionByName(b), filter(None, chat_dfs))
chat_nat_df = reduce(lambda a,b: a.unionByName(b), filter(None, chat_nat_dfs))

# ==================================================
# CHAT + CHAT NATURE JOIN
# ==================================================
chat_full_df = (
    chat_df.join(chat_nat_df, ["num_id", "mnth"], "left")
           .drop("num_id", "mnth")
)

# ==================================================
# AGGREGATE CALL (USER × CHANNEL)
# ==================================================
call_agg_df = (
    call_df
    .groupBy("user_id", "channel")
    .agg(
        count("*").alias("contact_count"),
        collect_set("primary_nature").alias("primary_nature"),
        collect_set("secondary_nature").alias("secondary_nature")
    )
    .withColumn("chat_nature", lit(None))
)

# ==================================================
# AGGREGATE CHAT (USER × CHANNEL)
# ==================================================
chat_agg_df = (
    chat_full_df
    .groupBy("user_id", "channel")
    .agg(
        count("*").alias("contact_count"),
        collect_set("chat_nature").alias("chat_nature")
    )
    .withColumn("primary_nature", lit(None))
    .withColumn("secondary_nature", lit(None))
)

# ==================================================
# UNION CALL + CHAT
# ==================================================
final_df = call_agg_df.unionByName(chat_agg_df)

# ==================================================
# CSV SAFE
# ==================================================
final_df = (
    final_df
    .withColumn("primary_nature", concat_ws("|", col("primary_nature")))
    .withColumn("secondary_nature", concat_ws("|", col("secondary_nature")))
    .withColumn("chat_nature", concat_ws("|", col("chat_nature")))
)

# ==================================================
# WRITE OUTPUT
# ==================================================
OUTPUT_DIR = "/user/2030435/CallCentreAnalystics/UnifiedCallChatNature"

final_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(OUTPUT_DIR)
