from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("ChannelFlowAnalysis_Final").getOrCreate()

mnth = "jan"
part = "202501"
post_login_values = ["OTP|S", "VB|S", "TPIN|S"]

# ------------------------------------------------------------------------------
# HDFS FILESYSTEM (FOR PATH CHECK)
# ------------------------------------------------------------------------------
hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

def path_exists(path):
    return hadoop_fs.exists(
        spark._jvm.org.apache.hadoop.fs.Path(path)
    )

# ------------------------------------------------------------------------------
# READERS
# ------------------------------------------------------------------------------
def read_stacy(path):
    df = spark.read.csv(path, header=True)
    ts_col = "HKT" if "HKT" in df.columns else "date (UTC)"
    uid_col = "user_id" if "user_id" in df.columns else "customer_id"

    return (
        df.withColumn("event_ts", to_timestamp(col(ts_col), "dd-MM-yyyy HH:mm"))
          .select(col(uid_col).alias("user_id"), "event_ts")
          .withColumn("event_channel", lit("Stacy"))
    )


def read_ivr(path):
    return (
        spark.read.csv(path, header=True)
        .filter(col("ONE_FA").isin(post_login_values))
        .withColumn("event_ts", to_timestamp(col("STARTTIME"), "dd-MM-yyyy HH:mm"))
        .select(col("REL_ID").alias("user_id"), "event_ts")
        .withColumn("event_channel", lit("IVR"))
    )


def read_call(path):
    return (
        spark.read.csv(path, header=True)
        .withColumn("event_ts", to_timestamp(col("Call Start Time"), "dd-MM-yyyy HH:mm"))
        .select(col("Customer No (CTI)").alias("user_id"), "event_ts")
        .withColumn("event_channel", lit("Call"))
    )


def read_chat(path):
    return (
        spark.read.csv(path, header=True)
        .filter(col("Pre/Post") == "Postlogin")
        .withColumn(
            "event_ts",
            to_timestamp(
                concat_ws(" ", col("Date28"), col("StartTime")),
                "dd-MM-yyyy HH:mm:ss"
            )
        )
        .select(col("REL ID").alias("user_id"), "event_ts")
        .withColumn("event_channel", lit("Chat"))
    )

# ------------------------------------------------------------------------------
# SAFE APPEND (ONLY IF FILE EXISTS)
# ------------------------------------------------------------------------------
dfs = []

def safe_append(reader_fn, path):
    if path_exists(path):
        dfs.append(reader_fn(path))
        print(f"[LOAD] {path}")
    else:
        print(f"[SKIP] {path} (not found)")

# ------------------------------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------------------------------
safe_append(read_stacy, f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_en_postlogin.csv")
safe_append(read_stacy, f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_zh_postlogin.csv")
safe_append(read_call,  f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv")
safe_append(read_chat,  f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv")

for i in range(1, 5):
    safe_append(read_ivr, f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv")

if not dfs:
    raise Exception("❌ No input files found. Job aborted.")

combined_df = (
    reduce(lambda a, b: a.unionByName(b), dfs)
    .dropna(subset=["user_id", "event_ts"])
)

# ------------------------------------------------------------------------------
# RANK EVENTS (CACHE ONCE)
# ------------------------------------------------------------------------------
w = Window.partitionBy("user_id").orderBy("event_ts")
ranked = combined_df.withColumn("rn", row_number().over(w)).cache()
ranked.count()

# ------------------------------------------------------------------------------
# REFERENCE (START) CHANNEL
# ------------------------------------------------------------------------------
starters = ranked.filter(col("rn") == 1) \
    .select("user_id", col("event_channel").alias("ref_channel"))

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
    .groupBy("ref_channel", "bucket")
    .count()
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
    return (
        df.filter(col("rn") == rn_value)
          .groupBy("ref_channel", "event_channel")
          .count()
          .withColumn("rnk", row_number().over(rank_win))
          .filter(col("rnk") <= 4)
          .groupBy("ref_channel")
          .agg(
              first(when(col("rnk") == 1, col("event_channel"))).alias(f"{prefix}_1"),
              first(when(col("rnk") == 2, col("event_channel"))).alias(f"{prefix}_2"),
              first(when(col("rnk") == 3, col("event_channel"))).alias(f"{prefix}_3"),
              first(when(col("rnk") == 4, col("event_channel"))).alias(f"{prefix}_4"),
              first(when(col("rnk") == 1, col("count"))).alias(f"{prefix}_count_1"),
              first(when(col("rnk") == 2, col("count"))).alias(f"{prefix}_count_2"),
              first(when(col("rnk") == 3, col("count"))).alias(f"{prefix}_count_3"),
              first(when(col("rnk") == 4, col("count"))).alias(f"{prefix}_count_4")
          )
    )

second_contacts = top_contacts(scoped, 2, "sec_con_chnl")
third_contacts  = top_contacts(scoped, 3, "third_con_chnl")

# ------------------------------------------------------------------------------
# FINAL OUTPUT (EXACT COLUMN ORDER)
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
