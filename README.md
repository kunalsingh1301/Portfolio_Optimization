from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

# --------------------------------------------------
# SPARK
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("PostLogin_Interaction_Level") \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
mnth = "jan"
part = "202501"
post_login_values = ["OTP|S", "VB|S", "TPIN|S"]

# --------------------------------------------------
# HDFS SAFE READ
# --------------------------------------------------
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

def path_exists(p):
    return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p))

def safe_read(reader, path):
    if path_exists(path):
        df = reader(path)
        if not df.rdd.isEmpty():
            return df
    return None

# --------------------------------------------------
# TIMESTAMP NORMALIZER (REUSED AS IS)
# --------------------------------------------------
def normalize_timestamp(df, ts_col):
    df = df.withColumn("_raw_ts", regexp_replace(trim(col(ts_col)), r"\s+", " "))

    df = df.withColumn("_excel",
        when(col("_raw_ts").rlike(r"^\d+(\.\d+)?$"),
             expr("timestampadd(SECOND, cast((_raw_ts-25569)*86400 as int), timestamp('1970-01-01'))"))
    )

    formats = [
        "dd-MM-yyyy HH:mm:ss","dd-MM-yyyy hh:mm:ss a",
        "dd/MM/yyyy HH:mm:ss","MM/dd/yyyy HH:mm:ss",
        "M/d/yy hh:mm a","M/d/yyyy hh:mm a"
    ]

    df = df.withColumn(
        "_string_ts",
        coalesce(*[to_timestamp(col("_raw_ts"), f) for f in formats])
    )

    return df.withColumn(
        "event_ts",
        coalesce(col("_excel"), col("_string_ts"))
    ).drop("_raw_ts","_excel","_string_ts")

# --------------------------------------------------
# READERS (POSTLOGIN ONLY)
# --------------------------------------------------
def read_stacy(path):
    df = spark.read.option("header", True).csv(path)
    df = normalize_timestamp(df, "HKT")
    return df.filter(col("user_id").isNotNull()) \
             .select(col("user_id"), col("event_ts")) \
             .withColumn("channel", lit("Stacy"))

def read_chat(path):
    df = spark.read.option("header", True).csv(path)
    df = normalize_timestamp(df, "Timestamp")
    return df.filter(col("Pre/Post")=="Postlogin") \
             .select(col("REL ID").alias("user_id"), col("event_ts")) \
             .withColumn("channel", lit("Chat"))

def read_call(path):
    df = spark.read.option("header", True).csv(path)
    df = normalize_timestamp(df, "Call Start Time")
    return df.filter(col("Verification Status")=="Pass") \
             .select(col("Customer No (CTI)").alias("user_id"), col("event_ts")) \
             .withColumn("channel", lit("Call"))

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------
dfs = []

for p, r in [
    (f"/user/2030435/CallCentreAnalystics/{mnth}_stacy.csv", read_stacy),
    (f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv", read_chat),
    (f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv", read_call)
]:
    df = safe_read(r, p)
    if df is not None:
        dfs.append(df)

combined_df = reduce(lambda a,b: a.unionByName(b), dfs) \
                .filter(col("event_ts").isNotNull()) \
                .cache()

# --------------------------------------------------
# GLOBAL TOTALS
# --------------------------------------------------
total_postlogin_contacts = combined_df.count()

# --------------------------------------------------
# ORDER INTERACTIONS PER USER
# --------------------------------------------------
w = Window.partitionBy("user_id").orderBy("event_ts")

df = combined_df.withColumn("interaction_idx", row_number().over(w))

# --------------------------------------------------
# FOLLOW-UP BUCKET (INTERACTION LEVEL)
# --------------------------------------------------
df = df.withColumn(
    "follow_bucket",
    when(col("interaction_idx")==1,"0")
    .when(col("interaction_idx")==2,"1")
    .when(col("interaction_idx")==3,"2")
    .otherwise("3+")
)

# --------------------------------------------------
# SECOND / THIRD CHANNEL (INTERACTION LEVEL)
# --------------------------------------------------
df = df.withColumn("second_channel",
    when(col("interaction_idx")>=2, col("channel"))
)

df = df.withColumn("third_channel",
    when(col("interaction_idx")>=3, col("channel"))
)

# --------------------------------------------------
# AGGREGATIONS
# --------------------------------------------------
total_case_df = df.groupBy("channel").agg(count("*").alias("total_case"))

bucket_df = (
    df.groupBy("channel","follow_bucket")
      .count()
      .groupBy("channel")
      .pivot("follow_bucket",["0","1","2","3+"])
      .sum("count")
      .fillna(0)
      .withColumnRenamed("0","follow_up_0")
      .withColumnRenamed("1","follow_up_1")
      .withColumnRenamed("2","follow_up_2")
      .withColumnRenamed("3+","follow_up_3+")
)

sec_df = (
    df.filter(col("interaction_idx")>=2)
      .groupBy("channel","second_channel")
      .count()
      .groupBy("channel")
      .pivot("second_channel",["Stacy","Chat","Call"])
      .sum("count")
      .fillna(0)
      .selectExpr(
          "channel",
          "Stacy as sec_stacy",
          "Chat as sec_chat",
          "Call as sec_call"
      )
)

third_df = (
    df.filter(col("interaction_idx")>=3)
      .groupBy("channel","third_channel")
      .count()
      .groupBy("channel")
      .pivot("third_channel",["Stacy","Chat","Call"])
      .sum("count")
      .fillna(0)
      .selectExpr(
          "channel",
          "Stacy as third_stacy",
          "Chat as third_chat",
          "Call as third_call"
      )
)

# --------------------------------------------------
# FINAL ASSEMBLY
# --------------------------------------------------
final_df = (
    total_case_df
    .join(bucket_df,"channel")
    .join(sec_df,"channel","left")
    .join(third_df,"channel","left")
    .withColumn("Date", lit(part))
    .withColumn("total_postlogin_contacts", lit(total_postlogin_contacts))
    .withColumnRenamed("channel","Channel")
)

final_df.show(truncate=False)
