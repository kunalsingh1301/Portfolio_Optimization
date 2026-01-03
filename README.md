from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("ChannelFlowAnalysis_Full_Robust").getOrCreate()

mnth = "jan"
part = "202501"
post_login_values = ["OTP|S", "VB|S", "TPIN|S"]

# ------------------------------------------------------------------------------
# HDFS FILESYSTEM
# ------------------------------------------------------------------------------
hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

def path_exists(path):
    return hadoop_fs.exists(
        spark._jvm.org.apache.hadoop.fs.Path(path)
    )

# ------------------------------------------------------------------------------
# ROBUST TIMESTAMP PARSER
# ------------------------------------------------------------------------------
def parse_timestamp(df, ts_col):
    df = df.withColumn(
        "event_ts",
        coalesce(
            # 24-hour formats with seconds
            to_timestamp(col(ts_col), "dd-MM-yyyy HH:mm:ss"),
            to_timestamp(col(ts_col), "dd-MM-yyyy HH:mm"),
            to_timestamp(col(ts_col), "d-M-yyyy HH:mm:ss"),
            to_timestamp(col(ts_col), "d-M-yyyy HH:mm"),
            to_timestamp(col(ts_col), "dd-M-yyyy HH:mm:ss"),
            to_timestamp(col(ts_col), "dd-M-yyyy HH:mm"),
            to_timestamp(col(ts_col), "d-M-yy HH:mm:ss"),
            to_timestamp(col(ts_col), "d-M-yy HH:mm"),
            # 12-hour AM/PM
            to_timestamp(col(ts_col), "d-M-yy hh:mm a"),
            to_timestamp(col(ts_col), "dd-M-yy hh:mm a"),
            to_timestamp(col(ts_col), "d-M-yyyy hh:mm a"),
            to_timestamp(col(ts_col), "dd-M-yyyy hh:mm a")
        )
    )
    # optional column to track parse failures
    df = df.withColumn("parse_failed", when(col("event_ts").isNull(), lit(1)).otherwise(lit(0)))
    return df

# ------------------------------------------------------------------------------
# READERS
# ------------------------------------------------------------------------------
def read_stacy(path):
    df = spark.read.csv(path, header=True)
    ts_col = "HKT" if "HKT" in df.columns else "date (UTC)"
    uid_col = "user_id" if "user_id" in df.columns else "customer_id"

    df = df.select(col(uid_col).alias("user_id"), col(ts_col))
    df = parse_timestamp(df, ts_col)
    df = df.withColumn("event_channel", lit("Stacy")).drop(ts_col)
    return df

def read_ivr(path):
    df = spark.read.csv(path, header=True)
    df = df.filter(col("ONE_FA").isin(post_login_values))
    df = df.select(col("REL_ID").alias("user_id"), col("STARTTIME"))
    df = parse_timestamp(df, "STARTTIME")
    df = df.withColumn("event_channel", lit("IVR")).drop("STARTTIME")
    return df

def read_call(path):
    df = spark.read.csv(path, header=True)
    df = df.select(col("Customer No (CTI)").alias("user_id"), col("Call Start Time"))
    df = parse_timestamp(df, "Call Start Time")
    df = df.withColumn("event_channel", lit("Call")).drop("Call Start Time")
    return df

def read_chat(path):
    df = spark.read.csv(path, header=True)
    df = df.filter(col("Pre/Post") == "Postlogin")
    df = df.select(col("REL ID").alias("user_id"), concat_ws(" ", col("Date28"), col("StartTime")).alias("chat_ts"))
    df = parse_timestamp(df, "chat_ts")
    df = df.withColumn("event_channel", lit("Chat")).drop("chat_ts")
    return df

# ------------------------------------------------------------------------------
# SAFE LOAD
# ------------------------------------------------------------------------------
dfs = []

def safe_append(reader, path):
    if path_exists(path):
        dfs.append(reader(path))
        print(f"[LOAD] {path}")
    else:
        print(f"[SKIP] {path}")

# Stacy
safe_append(read_stacy, f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_en_postlogin.csv")
safe_append(read_stacy, f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_zh_postlogin.csv")

# IVR
for i in range(1, 5):
    safe_append(read_ivr, f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv")

# Call & Chat
safe_append(read_call, f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv")
safe_append(read_chat, f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv")

if not dfs:
    raise Exception("No input files found")

# ------------------------------------------------------------------------------
# COMBINE DATA
# ------------------------------------------------------------------------------
combined_df = reduce(lambda a, b: a.unionByName(b), dfs)
combined_df = combined_df.dropna(subset=["user_id", "event_ts"]).cache()
combined_df.count()  # trigger parsing

# ------------------------------------------------------------------------------
# LOG FAILED PARSES
# ------------------------------------------------------------------------------
failed_count = combined_df.filter(col("parse_failed") == 1).count()
if failed_count > 0:
    print(f"Warning: {failed_count} rows failed timestamp parsing!")

# ------------------------------------------------------------------------------
# RANK EVENTS
# ------------------------------------------------------------------------------
w = Window.partitionBy("user_id").orderBy("event_ts")
ranked = combined_df.withColumn("rn", row_number().over(w))

# ------------------------------------------------------------------------------
# REFERENCE CHANNEL
# ------------------------------------------------------------------------------
starters = ranked.filter(col("rn") == 1).select("user_id", col("event_channel").alias("ref_channel"))
scoped = ranked.join(starters, "user_id")

# ------------------------------------------------------------------------------
# BASE METRICS
# ------------------------------------------------------------------------------
base_metrics = scoped.groupBy("ref_channel").agg(
    count("*").alias("Total_case"),
    countDistinct("user_id").alias("uniq_cust")
).withColumn(
    "rep_rate",
    (col("Total_case") - col("uniq_cust")) / col("Total_case") * 100
)

# ------------------------------------------------------------------------------
# FOLLOW-UP BUCKETS
# ------------------------------------------------------------------------------
followups = scoped.groupBy("ref_channel", "user_id").count()
followup_pivot = (
    followups.withColumn(
        "bucket",
        when(col("count") == 1, "follow_up_0")
        .when(col("count") == 2, "follow_up_1")
        .when(col("count") == 3, "follow_up_2")
        .otherwise("follow_up_3+")
    )
    .groupBy("ref_channel")
    .pivot("bucket", ["follow_up_0", "follow_up_1", "follow_up_2", "follow_up_3+"])
    .sum("count")
    .fillna(0)
)

# ------------------------------------------------------------------------------
# SECOND & THIRD CONTACT CHANNELS
# ------------------------------------------------------------------------------
rank_win = Window.partitionBy("ref_channel").orderBy(desc("count"))

def top_contacts(df, rn_value, prefix):
    ranked_contacts = (
        df.filter(col("rn") == rn_value)
          .groupBy("ref_channel", "event_channel")
          .count()
          .withColumn("rnk", row_number().over(rank_win))
          .filter(col("rnk") <= 4)
    )
    return ranked_contacts.groupBy("ref_channel").agg(
        max(when(col("rnk") == 1, col("event_channel"))).alias(f"{prefix}_1"),
        max(when(col("rnk") == 2, col("event_channel"))).alias(f"{prefix}_2"),
        max(when(col("rnk") == 3, col("event_channel"))).alias(f"{prefix}_3"),
        max(when(col("rnk") == 4, col("event_channel"))).alias(f"{prefix}_4"),
        max(when(col("rnk") == 1, col("count"))).alias(f"{prefix}_count_1"),
        max(when(col("rnk") == 2, col("count"))).alias(f"{prefix}_count_2"),
        max(when(col("rnk") == 3, col("count"))).alias(f"{prefix}_count_3"),
        max(when(col("rnk") == 4, col("count"))).alias(f"{prefix}_count_4")
    )

second_contacts = top_contacts(scoped, 2, "sec_con_chnl")
third_contacts  = top_contacts(scoped, 3, "third_con_chnl")

# ------------------------------------------------------------------------------
# FINAL OUTPUT
# ------------------------------------------------------------------------------
final_df = (
    base_metrics
    .join(followup_pivot, "ref_channel", "left")
    .join(second_contacts, "ref_channel", "left")
    .join(third_contacts, "ref_channel", "left")
    .withColumn("Date", lit(part))
    .withColumnRenamed("ref_channel", "Channel")
    .select(
        "Date", "Channel", "Total_case", "uniq_cust", "rep_rate",
        "follow_up_0", "follow_up_1", "follow_up_2", "follow_up_3+",
        "sec_con_chnl_1", "sec_con_chnl_2", "sec_con_chnl_3", "sec_con_chnl_4",
        "sec_con_chnl_count_1", "sec_con_chnl_count_2", "sec_con_chnl_count_3", "sec_con_chnl_count_4",
        "third_con_chnl_1", "third_con_chnl_2", "third_con_chnl_3", "third_con_chnl_4",
        "third_con_chnl_count_1", "third_con_chnl_count_2", "third_con_chnl_count_3", "third_con_chnl_count_4"
    )
)

# ------------------------------------------------------------------------------
# SHOW RESULT
# ------------------------------------------------------------------------------
final_df.show(truncate=False)
