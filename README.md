from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ============================================================
# INPUT ASSUMPTION
# ------------------------------------------------------------
# combined_df has the following columns:
#   user_id | event_ts | channel
#
# event_ts is TIMESTAMP
# ============================================================

# ---------------- CONFIG ----------------
part = "202503"   # reporting month

# ============================================================
# 1. ORDER EVENTS PER USER
# ============================================================
w_user = Window.partitionBy("user_id").orderBy("event_ts")

df = combined_df.withColumn("rn", row_number().over(w_user))

# ============================================================
# 2. STARTER CHANNEL (rn = 1)
# ============================================================
starter_df = (
    df.filter(col("rn") == 1)
      .select("user_id", col("channel").alias("starter_channel"))
)

df = df.join(starter_df, "user_id")

# ============================================================
# 3. CASE ID (1 case = 1 starter + its follow-ups)
# ============================================================
case_w = Window.partitionBy("user_id").orderBy("event_ts")

df = df.withColumn(
    "case_id",
    sum(when(col("rn") == 1, 1).otherwise(0)).over(case_w)
)

# ============================================================
# 4. CASE SIZE (number of interactions in a case)
# ============================================================
case_cnt_df = (
    df.groupBy("starter_channel", "user_id", "case_id")
      .agg(count("*").alias("case_size"))
)

# ============================================================
# 5. FOLLOW-UP BUCKETS (CASE LEVEL)
# ============================================================
bucket_df = case_cnt_df.withColumn(
    "bucket",
    when(col("case_size") == 1, "0")       # no follow-up
    .when(col("case_size") == 2, "1")       # 1 follow-up
    .when(col("case_size") == 3, "2")       # 2 follow-ups
    .otherwise("3+")                         # 3+ follow-ups
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

# ============================================================
# 6. CASE-LEVEL REP RATE
# % of cases that had >= 1 follow-up
# ============================================================
total_case_df = (
    df.filter(col("rn") == 1)
      .groupBy("starter_channel")
      .agg(count("*").alias("total_case"))
)

repeat_case_df = (
    case_cnt_df.filter(col("case_size") > 1)
    .groupBy("starter_channel")
    .agg(count("*").alias("repeat_case"))
)

rep_df = (
    total_case_df
    .join(repeat_case_df, "starter_channel", "left")
    .fillna(0)
    .withColumn("rep_rate", col("repeat_case") * 100 / col("total_case"))
)

# ============================================================
# 7. SECONDARY / THIRD CHANNEL (CASE LEVEL)
# ============================================================
def top_contact_case(df, rn, prefix, top_n=4):
    nth_df = df.filter(col("rn") == rn)

    base = (
        nth_df.groupBy("starter_channel", "channel")
              .agg(
                  countDistinct(
                      struct("user_id", "case_id")
                  ).alias("case_count")
              )
    )

    w = Window.partitionBy("starter_channel").orderBy(col("case_count").desc())
    ranked = base.withColumn("rank", row_number().over(w)).filter(col("rank") <= top_n)

    exprs = []
    for i in range(1, top_n + 1):
        exprs += [
            max(when(col("rank") == i, col("channel"))).alias(f"{prefix}_chnl_{i}"),
            max(when(col("rank") == i, col("case_count"))).alias(f"{prefix}_chnl_count_{i}")
        ]

    return ranked.groupBy("starter_channel").agg(*exprs)

sec_df   = top_contact_case(df, 2, "sec")
third_df = top_contact_case(df, 3, "third")

# ============================================================
# 8. FINAL JOIN
# ============================================================
final_df = (
    rep_df
    .join(bucket_pivot, "starter_channel", "left")
    .join(sec_df, "starter_channel", "left")
    .join(third_df, "starter_channel", "left")
    .withColumnRenamed("starter_channel", "Channel")
    .withColumn("Date", lit(part))
)

# ============================================================
# 9. FINAL COLUMN ORDER (SAFE)
# ============================================================
columns = [
    "Date", "Channel",
    "total_case", "repeat_case", "rep_rate",
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

# ============================================================
# 10. SHOW RESULT
# ============================================================
final_df.show(truncate=False)
