# ============================================================
# 2️⃣ CASE CONSTRUCTION (CASE LEVEL)
# ============================================================
w_user = Window.partitionBy("user_id").orderBy("event_ts")

df = combined_df.withColumn("rn", row_number().over(w_user))

starter_df = (
    df.filter(col("rn") == 1)
      .select("user_id", col("channel").alias("starter_channel"))
)

df = df.join(starter_df, "user_id")

df = df.withColumn(
    "case_id",
    sum(when(col("rn") == 1, 1).otherwise(0)).over(w_user)
)

# ============================================================
# 3️⃣ CASE SIZE (PER CASE)
# ============================================================
case_df = (
    df.groupBy("starter_channel", "user_id", "case_id")
      .agg(count("*").alias("case_size"))
)

# ============================================================
# 4️⃣ TOTAL CASES + CUSTOMERS (STARTER CHANNEL)
# ============================================================
total_case_df = (
    case_df.groupBy("starter_channel")
    .agg(
        count("*").alias("total_case"),
        countDistinct("user_id").alias("starter_customers")
    )
)

# ============================================================
# 5️⃣ FOLLOW-UP BUCKETS (CASE-LEVEL ❗ FIXED)
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
    .agg(sum("case_size").alias("case_volume"))   # 🔥 SUM OF CASES
    .groupBy("starter_channel")
    .pivot("bucket", ["0", "1", "2", "3+"])
    .sum("case_volume")
    .fillna(0)
    .withColumnRenamed("0", "follow_up_0")
    .withColumnRenamed("1", "follow_up_1")
    .withColumnRenamed("2", "follow_up_2")
    .withColumnRenamed("3+", "follow_up_3+")
)

# ============================================================
# 6️⃣ REPEAT RATE (CUSTOMER LEVEL – OK)
# ============================================================
cust_case_cnt = (
    case_df.groupBy("starter_channel", "user_id")
    .agg(count("*").alias("cases_started"))
)

repeat_df = (
    cust_case_cnt.filter(col("cases_started") > 1)
    .groupBy("starter_channel")
    .agg(countDistinct("user_id").alias("repeat_customers"))
)

rep_df = (
    total_case_df
    .join(repeat_df, "starter_channel", "left")
    .fillna(0)
    .withColumn(
        "rep_rate",
        col("repeat_customers") * 100 / col("starter_customers")
    )
)

# ============================================================
# 7️⃣ 2ND / 3RD CONTACT CHANNEL (CASE-LEVEL ❗ FIXED)
# ============================================================
def top_channel_case_level(df, rn, prefix):
    base = (
        df.filter(col("rn") == rn)
        .groupBy("starter_channel", "channel")
        .agg(countDistinct(struct("user_id", "case_id")).alias("case_count"))
    )

    w = Window.partitionBy("starter_channel").orderBy(col("case_count").desc())

    ranked = base.withColumn("rank", row_number().over(w)).filter(col("rank") <= 4)

    exprs = []
    for i in range(1, 5):
        exprs.append(max(when(col("rank") == i, col("channel"))).alias(f"{prefix}_chnl_{i}"))
        exprs.append(max(when(col("rank") == i, col("case_count"))).alias(f"{prefix}_chnl_count_{i}"))

    return ranked.groupBy("starter_channel").agg(*exprs)

sec_df = top_channel_case_level(df, 2, "sec")
third_df = top_channel_case_level(df, 3, "third")

# ============================================================
# 8️⃣ FINAL OUTPUT
# ============================================================
final_df = (
    rep_df
    .join(bucket_pivot, "starter_channel")
    .join(sec_df, "starter_channel", "left")
    .join(third_df, "starter_channel", "left")
    .withColumnRenamed("starter_channel", "Channel")
    .withColumn("Date", lit(part))
    .withColumn("total_postlogin_cases", lit(total_postlogin_cases))
    .withColumn("total_distinct_users", lit(total_distinct_users))
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

for c in columns:
    if c not in final_df.columns:
        final_df = final_df.withColumn(c, lit(None))

final_df = final_df.select(columns)

final_df.show(truncate=False)
