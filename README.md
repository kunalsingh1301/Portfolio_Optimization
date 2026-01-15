from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# 1️⃣ CASE CONSTRUCTION (USER → MULTIPLE CASES)
# ============================================================
w_user = Window.partitionBy("user_id").orderBy("event_ts")

df = combined_df.withColumn("rn", F.row_number().over(w_user))

starter_df = (
    df.filter(F.col("rn") == 1)
      .select("user_id", F.col("channel").alias("starter_channel"))
)

df = df.join(starter_df, "user_id")

df = df.withColumn(
    "case_id",
    F.sum(F.when(F.col("rn") == 1, 1).otherwise(0)).over(w_user)
)

# ============================================================
# 2️⃣ CASE SIZE (ONE ROW = ONE CASE)
# ============================================================
case_df = (
    df.groupBy("starter_channel", "user_id", "case_id")
      .agg(F.count("*").alias("case_size"))
)

# ============================================================
# 3️⃣ FOLLOW-UP BUCKETS (CASE-LEVEL)
# ============================================================
bucket_df = case_df.withColumn(
    "bucket",
    F.when(F.col("case_size") == 1, "0")
     .when(F.col("case_size") == 2, "1")
     .when(F.col("case_size") == 3, "2")
     .otherwise("3+")
)

bucket_pivot = (
    bucket_df.groupBy("starter_channel", "bucket")
    .count()                                  # ← number of CASES
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
# 4️⃣ TOTAL CASES (DERIVED, NOT RECOUNTED)
# ============================================================
total_case_df = bucket_pivot.withColumn(
    "total_case",
    F.col("follow_up_0")
  + F.col("follow_up_1")
  + F.col("follow_up_2")
  + F.col("follow_up_3+")
)

# ============================================================
# 5️⃣ SECOND CHANNEL (CASE-LEVEL, rn = 2)
# ============================================================
sec_base = (
    df.filter(F.col("rn") == 2)
      .groupBy("starter_channel", "channel")
      .agg(F.countDistinct(F.struct("user_id", "case_id")).alias("case_count"))
)

w2 = Window.partitionBy("starter_channel").orderBy(F.col("case_count").desc())

sec_ranked = sec_base.withColumn("rank", F.row_number().over(w2)).filter("rank <= 4")

sec_df = sec_ranked.groupBy("starter_channel").agg(
    F.max(F.when(F.col("rank") == 1, F.col("channel"))).alias("sec_chnl_1"),
    F.max(F.when(F.col("rank") == 2, F.col("channel"))).alias("sec_chnl_2"),
    F.max(F.when(F.col("rank") == 3, F.col("channel"))).alias("sec_chnl_3"),
    F.max(F.when(F.col("rank") == 4, F.col("channel"))).alias("sec_chnl_4"),
    F.max(F.when(F.col("rank") == 1, F.col("case_count"))).alias("sec_chnl_count_1"),
    F.max(F.when(F.col("rank") == 2, F.col("case_count"))).alias("sec_chnl_count_2"),
    F.max(F.when(F.col("rank") == 3, F.col("case_count"))).alias("sec_chnl_count_3"),
    F.max(F.when(F.col("rank") == 4, F.col("case_count"))).alias("sec_chnl_count_4")
)

# ============================================================
# 6️⃣ THIRD CHANNEL (CASE-LEVEL, rn = 3)
# ============================================================
third_base = (
    df.filter(F.col("rn") == 3)
      .groupBy("starter_channel", "channel")
      .agg(F.countDistinct(F.struct("user_id", "case_id")).alias("case_count"))
)

w3 = Window.partitionBy("starter_channel").orderBy(F.col("case_count").desc())

third_ranked = third_base.withColumn("rank", F.row_number().over(w3)).filter("rank <= 4")

third_df = third_ranked.groupBy("starter_channel").agg(
    F.max(F.when(F.col("rank") == 1, F.col("channel"))).alias("third_chnl_1"),
    F.max(F.when(F.col("rank") == 2, F.col("channel"))).alias("third_chnl_2"),
    F.max(F.when(F.col("rank") == 3, F.col("channel"))).alias("third_chnl_3"),
    F.max(F.when(F.col("rank") == 4, F.col("channel"))).alias("third_chnl_4"),
    F.max(F.when(F.col("rank") == 1, F.col("case_count"))).alias("third_chnl_count_1"),
    F.max(F.when(F.col("rank") == 2, F.col("case_count"))).alias("third_chnl_count_2"),
    F.max(F.when(F.col("rank") == 3, F.col("case_count"))).alias("third_chnl_count_3"),
    F.max(F.when(F.col("rank") == 4, F.col("case_count"))).alias("third_chnl_count_4")
)

# ============================================================
# 7️⃣ FINAL OUTPUT (FULLY RECONCILED)
# ============================================================
final_df = (
    total_case_df
    .join(sec_df, "starter_channel", "left")
    .join(third_df, "starter_channel", "left")
    .withColumnRenamed("starter_channel", "Channel")
    .withColumn("Date", F.lit(part))
)

final_df.show(truncate=False)
