from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("ChannelFlowAnalysis_Optimized").getOrCreate()

mnth = "jan"
part = "202501"
post_login_values = ["OTP|S", "VB|S", "TPIN|S"]

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
          .withColumn("channel", lit("Stacy"))
    )


def read_ivr(path):
    return (
        spark.read.csv(path, header=True)
        .filter(col("ONE_FA").isin(post_login_values))
        .withColumn("event_ts", to_timestamp(col("STARTTIME"), "dd-MM-yyyy HH:mm"))
        .select(col("REL_ID").alias("user_id"), "event_ts")
        .withColumn("channel", lit("IVR"))
    )


def read_call(path):
    return (
        spark.read.csv(path, header=True)
        .withColumn("event_ts", to_timestamp(col("Call Start Time"), "dd-MM-yyyy HH:mm"))
        .select(col("Customer No (CTI)").alias("user_id"), "event_ts")
        .withColumn("channel", lit("Call"))
    )


def read_chat(path):
    return (
        spark.read.csv(path, header=True)
        .filter(col("Pre/Post") == "Postlogin")
        .withColumn(
            "event_ts",
            to_timestamp(concat_ws(" ", col("Date28"), col("StartTime")),
                         "dd-MM-yyyy HH:mm:ss")
        )
        .select(col("REL ID").alias("user_id"), "event_ts")
        .withColumn("channel", lit("Chat"))
    )

# ------------------------------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------------------------------
dfs = [
    read_stacy(f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_en_postlogin.csv"),
    read_stacy(f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_zh_postlogin.csv"),
    read_call(f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"),
    read_chat(f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv")
]

for i in range(1, 5):
    dfs.append(read_ivr(f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv"))

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
# START CHANNEL
# ------------------------------------------------------------------------------
starters = ranked.filter(col("rn") == 1) \
    .select("user_id", col("channel").alias("Channel"))

scoped = ranked.join(starters, "user_id")

# ------------------------------------------------------------------------------
# BASE METRICS
# ------------------------------------------------------------------------------
base_metrics = scoped.groupBy("Channel").agg(
    count("*").alias("Total_case"),
    countDistinct("user_id").alias("uniq_cust")
).withColumn(
    "rep_rate",
    (col("Total_case") - col("uniq_cust")) / col("Total_case") * 100
)

# ------------------------------------------------------------------------------
# FOLLOW-UP BUCKETS
# ------------------------------------------------------------------------------
followups = scoped.groupBy("Channel", "user_id").count()

followup_pivot = (
    followups.withColumn(
        "bucket",
        when(col("count") == 1, "follow_up_0")
        .when(col("count") == 2, "follow_up_1")
        .when(col("count") == 3, "follow_up_2")
        .otherwise("follow_up_3+")
    )
    .groupBy("Channel", "bucket")
    .count()
    .groupBy("Channel")
    .pivot("bucket", ["follow_up_0", "follow_up_1", "follow_up_2", "follow_up_3+"])
    .sum("count")
    .fillna(0)
)

# ------------------------------------------------------------------------------
# SECOND & THIRD CONTACTS
# ------------------------------------------------------------------------------
rank_win = Window.partitionBy("Channel").orderBy(desc("count"))

def top_contacts(df, rn_value, prefix):
    return (
        df.filter(col("rn") == rn_value)
          .groupBy("Channel", "channel")
          .count()
          .withColumn("rnk", row_number().over(rank_win))
          .filter(col("rnk") <= 4)
          .groupBy("Channel")
          .agg(
              first(when(col("rnk") == 1, col("channel"))).alias(f"{prefix}_1"),
              first(when(col("rnk") == 2, col("channel"))).alias(f"{prefix}_2"),
              first(when(col("rnk") == 3, col("channel"))).alias(f"{prefix}_3"),
              first(when(col("rnk") == 4, col("channel"))).alias(f"{prefix}_4"),
              first(when(col("rnk") == 1, col("count"))).alias(f"{prefix}_count_1"),
              first(when(col("rnk") == 2, col("count"))).alias(f"{prefix}_count_2"),
              first(when(col("rnk") == 3, col("count"))).alias(f"{prefix}_count_3"),
              first(when(col("rnk") == 4, col("count"))).alias(f"{prefix}_count_4")
          )
    )

second_contacts = top_contacts(scoped, 2, "sec_con_chnl")
third_contacts  = top_contacts(scoped, 3, "third_con_chnl")

# ------------------------------------------------------------------------------
# FINAL OUTPUT
# ------------------------------------------------------------------------------
final_df = (
    base_metrics
    .join(followup_pivot, "Channel", "left")
    .join(second_contacts, "Channel", "left")
    .join(third_contacts, "Channel", "left")
    .withColumn("Date", lit(part))
    .select(
        "Date", "Channel", "Total_case", "uniq_cust", "rep_rate",
        "follow_up_0", "follow_up_1", "follow_up_2", "follow_up_3+",
        "sec_con_chnl_1", "sec_con_chnl_2", "sec_con_chnl_3", "sec_con_chnl_4",
        "sec_con_chnl_count_1", "sec_con_chnl_count_2", "sec_con_chnl_count_3", "sec_con_chnl_count_4",
        "third_con_chnl_1", "third_con_chnl_2", "third_con_chnl_3", "third_con_chnl_4",
        "third_con_chnl_count_1", "third_con_chnl_count_2", "third_con_chnl_count_3", "third_con_chnl_count_4"
    )
)

final_df.show(truncate=False)
