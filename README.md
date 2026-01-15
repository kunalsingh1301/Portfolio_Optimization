from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ============================================================
# 1️⃣ GLOBAL TOTALS (INTERACTION LEVEL)
# ============================================================
total_postlogin_cases = combined_df.count()
total_distinct_users = combined_df.select("user_id").distinct().count()

# ============================================================
# 2️⃣ ORDER INTERACTIONS PER USER
# ============================================================
w_user = Window.partitionBy("user_id").orderBy("event_ts")

df = combined_df.withColumn("rn", row_number().over(w_user))

# ============================================================
# 3️⃣ DEFINE CASES
# ------------------------------------------------------------
# Each user has exactly ONE case per month
# (This matches your stakeholder expectations)
# ============================================================
df = df.withColumn("case_id", lit(1))

# ============================================================
# 4️⃣ STARTER CHANNEL (CASE LEVEL)
# ============================================================
starter_df = (
    df.filter(col("rn") == 1)
      .select("user_id", "case_id", col("channel").alias("starter_channel"))
)

df = df.join(starter_df, ["user_id", "case_id"], "left")

# ============================================================
# 5️⃣ CASE SIZE
# ============================================================
case_df = (
    df.groupBy("starter_channel", "user_id", "case_id")
      .agg(count("*").alias("case_size"))
)

# ============================================================
# 6️⃣ TOTAL CASES + STARTER CUSTOMERS
# ============================================================
total_case_df = (
    case_df.groupBy("starter_channel")
    .agg(
        count("*").alias("total_case"),
        countDistinct("user_id").alias("starter_customers")
    )
)

# ============================================================
# 7️⃣ FOLLOW-UP BUCKETS (CASE LEVEL)
# ============================================================
bucket_df = case_df.withColumn(
    "bucket",
    when(col("case_size") == 1, "0")
    .when(col("case_size") == 2, "1")
    .when(col("case_size") == 3, "2")
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
# 8️⃣ SECOND & THIRD CHANNEL (CASE LEVEL)
# ============================================================
def channel_rank(df, rn_value, prefix):
    base = (
        df.filter(col("rn") == rn_value)
          .groupBy("starter_channel", "channel")
          .agg(countDistinct(struct("user_id", "case_id")).alias("case_count"))
    )

    w = Window.partitionBy("starter_channel").orderBy(col("case_count").desc())
    ranked = base.withColumn("r", row_number().over(w)).filter(col("r") <= 4)

    exprs = []
    for i in range(1, 5):
        exprs += [
            max(when(col("r") == i, col("channel"))).alias(f"{prefix}_chnl_{i}"),
            max(when(col("r") == i, col("case_count"))).alias(f"{prefix}_chnl_count_{i}")
        ]

    return ranked.groupBy("starter_channel").agg(*exprs)

sec_df   = channel_rank(df, 2, "sec")
third_df = channel_rank(df, 3, "third")

# ============================================================
# 9️⃣ REPEAT RATE (CASE-BASED, CLEAN)
# ============================================================
rep_df = (
    total_case_df
    .join(bucket_pivot, "starter_channel")
    .withColumn(
        "rep_rate",
        (col("follow_up_1") + col("follow_up_2") + col("follow_up_3+")) / col("total_case")
    )
)

# ============================================================
# 🔟 FINAL ASSEMBLY
# ============================================================
final_df = (
    rep_df
    .join(sec_df, "starter_channel", "left")
    .join(third_df, "starter_channel", "left")
    .withColumnRenamed("starter_channel", "Channel")
    .withColumn("Date", lit(part))
    .withColumn("total_postlogin_cases", lit(total_postlogin_cases))
    .withColumn("total_distinct_users", lit(total_distinct_users))
)

# ============================================================
# FINAL COLUMN ORDER
# ============================================================
columns = [
    "Date","Channel",
    "total_postlogin_cases","total_distinct_users",
    "total_case","starter_customers","rep_rate",
    "follow_up_0","follow_up_1","follow_up_2","follow_up_3+",
    "sec_chnl_1","sec_chnl_2","sec_chnl_3","sec_chnl_4",
    "sec_chnl_count_1","sec_chnl_count_2","sec_chnl_count_3","sec_chnl_count_4",
    "third_chnl_1","third_chnl_2","third_chnl_3","third_chnl_4",
    "third_chnl_count_1","third_chnl_count_2","third_chnl_count_3","third_chnl_count_4"
]

for c in columns:
    if c not in final_df.columns:
        final_df = final_df.withColumn(c, lit(None))

final_df.select(columns).show(truncate=False)
