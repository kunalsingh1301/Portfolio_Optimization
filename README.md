# ============================================================
# 2️⃣ CASE CONSTRUCTION (CASE LEVEL) — FIXED
# ============================================================
w_user = Window.partitionBy("user_id").orderBy("event_ts")

# Step 1: Assign row number per user (rn)
df = combined_df.withColumn("rn", row_number().over(w_user))

# Step 2: Determine starter channel for each user
starter_df = (
    df.filter(col("rn") == 1)
      .select("user_id", col("channel").alias("starter_channel"))
)

df = df.join(starter_df, "user_id")

# Step 3: Assign case_id per user — increment at first contact
df = df.withColumn(
    "case_id",
    sum(when(col("rn") == 1, 1).otherwise(0)).over(w_user)
)
