from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# GLOBAL TOTALS
# ============================================================
total_postlogin_cases = combined_df.count()
total_distinct_users = combined_df.select("user_id").distinct().count()

# ============================================================
# 1️⃣ CASE CONSTRUCTION (CASE-LEVEL)
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
# 2️⃣ CASE TABLE (1 ROW = 1 CASE)
# ============================================================
case_df = (
    df.groupBy("starter_channel", "user_id", "case_id")
      .agg(F.count("*").alias("case_size"))
)

# ============================================================
# 3️⃣ STARTER CUSTOMERS (FIXED)
# ============================================================
starter_customer_df = (
    case_df.groupBy("starter_channel")
    .agg(F.countDistinct("user_id").alias("starter_customers"))
)

# ============================================================
# 4️⃣ FOLLOW-UP BUCKETS (CASE-LEVEL)
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
# 5️⃣ TOTAL CASES (DERIVED, GUARANTEED MATCH)
# ============================================================
total_case_df = bucket_pivot.withColumn(
    "total_case",
    F.col("follow_up_0")
  + F.col("follow_up_1")
  + F.col("follow_up_2")
  + F.col("follow_up_3+")
)

rep_rate_df = total_case_df.withColumn(
    "rep_rate",
    (F.col("follow_up_1") + F.col("follow_up_2") + F.col("follow_up_3+"))
    / F.col("total_case")
)


# ============================================================
# 6️⃣ SECOND CHANNEL (CASE-LEVEL)
# ============================================================
sec_base = (
    df.filter(F.col("rn") == 2)
      .groupBy("starter_channel", "channel")
      .agg(F.countDistinct(F.struct("user_id", "case_id")).alias("case_count"))
)

w2 = Window.partitionBy("starter_channel").orderBy(F.col("case_count").desc())

sec_ranked = sec_base.withColumn("rank", F.row_number().over(w2)).filter("rank <= 4")

sec_df = sec_ranked.groupBy("starter_channel").agg(
    *[F.max(F.when(F.col("rank") == i, F.col("channel"))).alias(f"sec_chnl_{i}") for i in range(1,5)],
    *[F.max(F.when(F.col("rank") == i, F.col("case_count"))).alias(f"sec_chnl_count_{i}") for i in range(1,5)]
)

# ============================================================
# 7️⃣ THIRD CHANNEL (CASE-LEVEL)
# ============================================================
third_base = (
    df.filter(F.col("rn") == 3)
      .groupBy("starter_channel", "channel")
      .agg(F.countDistinct(F.struct("user_id", "case_id")).alias("case_count"))
)

w3 = Window.partitionBy("starter_channel").orderBy(F.col("case_count").desc())

third_ranked = third_base.withColumn("rank", F.row_number().over(w3)).filter("rank <= 4")

third_df = third_ranked.groupBy("starter_channel").agg(
    *[F.max(F.when(F.col("rank") == i, F.col("channel"))).alias(f"third_chnl_{i}") for i in range(1,5)],
    *[F.max(F.when(F.col("rank") == i, F.col("case_count"))).alias(f"third_chnl_count_{i}") for i in range(1,5)]
)

# ============================================================
# 8️⃣ FINAL OUTPUT (ORDER EXACT AS REQUESTED)
# ============================================================
final_df = (
    total_case_df
    .join(starter_customer_df, "starter_channel")
    .join(sec_df, "starter_channel", "left")
    .join(third_df, "starter_channel", "left")
    .withColumnRenamed("starter_channel", "Channel")
    .withColumn("Date", F.lit(part))
    .withColumn("total_postlogin_cases", F.lit(total_postlogin_cases))
    .withColumn("total_distinct_users", F.lit(total_distinct_users))
    .withColumn("rep_rate", F.lit(None))  # kept for schema compatibility
)

columns = [
    "Date", "Channel",
    "total_postlogin_cases", "total_distinct_users",
    "total_case", "starter_customers", "rep_rate",
    "follow_up_0", "follow_up_1", "follow_up_2", "follow_up_3+",
    "sec_chnl_1", "sec_chnl_2", "sec_chnl_3", "sec_chnl_4",
    "sec_chnl_count_1", "sec_chnl_count_2", "sec_chnl_count_3", "sec_chnl_count_4",
    "third_chnl_1", "third_chnl_2", "third_chnl_3", "third_chnl_4",
    "third_chnl_count_1", "third_chnl_count_2", "third_chnl_count_3", "third_chnl_count_4"
]

final_df.select(columns).show(truncate=False)
