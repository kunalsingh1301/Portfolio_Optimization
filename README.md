from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

# ======================================================
# SPARK SESSION
# ======================================================
spark = SparkSession.builder.appName("ChannelFlowDebugSingleFile").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ======================================================
# ASSUMPTION:
# combined_df ALREADY EXISTS with columns:
# user_id | channel | event_ts
# ======================================================

# ======================================================
# CLEAN & ORDER JOURNEY
# ======================================================
combined_df = combined_df.dropna(subset=["user_id", "event_ts"])

w = Window.partitionBy("user_id").orderBy("event_ts")

df = combined_df.withColumn("rn", row_number().over(w))

starter_df = (
    df.filter(col("rn") == 1)
      .select("user_id", col("channel").alias("starter_channel"))
)

df = df.join(starter_df, "user_id")

print("===== FULL USER JOURNEY (DEBUG) =====")
df.orderBy("user_id", "rn").show(500, False)

# ======================================================
# TOTAL + REP RATE (UNCHANGED)
# ======================================================
agg_df = df.groupBy("starter_channel").agg(
    count("*").alias("total_case"),
    countDistinct("user_id").alias("uniq_cust")
).withColumn(
    "rep_rate",
    (col("total_case") - col("uniq_cust")) * 100 / col("total_case")
)

print("===== AGG SUMMARY =====")
agg_df.show(False)

# ======================================================
# FOLLOW-UP BUCKETS (UNCHANGED)
# ======================================================
follow_df = df.groupBy("starter_channel", "user_id").count()

bucket_df = follow_df.withColumn(
    "bucket",
    when(col("count") == 1, "0")
    .when(col("count") == 2, "1")
    .when(col("count") == 3, "2")
    .otherwise("3+")
)

bucket_pivot = (
    bucket_df.groupBy("starter_channel", "bucket")
    .count()
    .groupBy("starter_channel")
    .pivot("bucket", ["0", "1", "2", "3+"])
    .sum("count")
    .fillna(0)
    .withColumnRenamed("0", "follow_up_0")
    .withColumnRenamed("1", "follow_up_1")
    .withColumnRenamed("2", "follow_up_2")
    .withColumnRenamed("3+", "follow_up_3+")
)

print("===== FOLLOW-UP BUCKETS =====")
bucket_pivot.show(False)

# ======================================================
# SECOND / THIRD CONTACT LOGIC (FIXED + DEBUG)
# ======================================================
def top_contact_fixed(df, rn, prefix, top_n=4):

    nth_df = df.filter(col("rn") == rn)

    base = (
        nth_df
        .groupBy("starter_channel", "channel")
        .agg(countDistinct("user_id").alias("user_count"))
    )

    w = Window.partitionBy("starter_channel").orderBy(col("user_count").desc())

    ranked = (
        base.withColumn("rank", row_number().over(w))
            .filter(col("rank") <= top_n)
    )

    print(f"===== {prefix.upper()} CONTACT DEBUG (rn={rn}) =====")
    ranked.orderBy("starter_channel", "rank").show(200, False)

    exprs = []
    for i in range(1, top_n + 1):
        exprs += [
            max(when(col("rank") == i, col("channel"))).alias(f"{prefix}_chnl_{i}"),
            max(when(col("rank") == i, col("user_count"))).alias(f"{prefix}_chnl_count_{i}")
        ]

    return ranked.groupBy("starter_channel").agg(*exprs)

sec_df = top_contact_fixed(df, 2, "sec")
third_df = top_contact_fixed(df, 3, "third")

# ======================================================
# FINAL JOIN
# ======================================================
part = "202501"

final_df = (
    agg_df
    .join(bucket_pivot, "starter_channel", "left")
    .join(sec_df, "starter_channel", "left")
    .join(third_df, "starter_channel", "left")
    .withColumnRenamed("starter_channel", "Channel")
    .withColumn("Date", lit(part))
)

# ======================================================
# FIX OUTPUT SCHEMA
# ======================================================
columns = [
    "Date", "Channel",
    "total_case", "uniq_cust", "rep_rate",
    "follow_up_0", "follow_up_1", "follow_up_2", "follow_up_3+",
    "sec_chnl_1", "sec_chnl_2", "sec_chnl_3", "sec_chnl_4",
    "sec_chnl_count_1", "sec_chnl_count_2", "sec_chnl_count_3", "sec_chnl_count_4",
    "third_chnl_1", "third_chnl_2", "third_chnl_3", "third_chnl_4",
    "third_chnl_count_1", "third_chnl_count_2", "third_chnl_count_3", "third_chnl_count_4"
]

for c in columns:
    if c not in final_df.columns:
        final_df = final_df.withColumn(c, lit(None))

final_df = final_df.select(columns)

# ======================================================
# FINAL OUTPUT (HUE)
# ======================================================
print("===== FINAL RESULT =====")
final_df.show(truncate=False)
