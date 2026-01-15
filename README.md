from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

# ---------------- SPARK SESSION ----------------
spark = SparkSession.builder.appName("PostLoginFlow_InteractionLevel").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ---------------- CONFIG ----------------
mnth = "oct"
part = "202510"

stacy_cnt = ["en", "zh"]
stacy_auth = ["post"]

# ---------------- HDFS HELPERS ----------------
hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

def path_exists(p):
    return hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p))

# ---------------- TIMESTAMP NORMALIZER (YOUR WORKING VERSION) ----------------
def normalize_timestamp(df, ts_col):
    if ts_col == "Call Start Time":
      df = df.withColumn("_ts", regexp_replace(trim(col(ts_col)),r"\s+"," "))
    
      # Step 1: Handle space-separated numeric Excel values (e.g., "45231 0.5625")
      df = df.withColumn(
          "_ts",
          when(
              col("_ts").rlike(r"^\d+\s+\d+(\.\d+)?$"),
              regexp_replace(col("_ts"), r"(\d+)\s+(\d+(\.\d+)?)$",r"\1.\2")
          ).otherwise(col("_ts"))
      )
      
      # Step 2: Convert Excel numeric dates to timestamp (e.g., "45231.5625")
      df = df.withColumn(
          "_ts",
          when(
              col("_ts").rlike(r"^\d+(\.\d+)?$"),
              from_unixtime(unix_timestamp(lit("1899-12-30"))+(col("_ts").cast("double")*86400))
          ).otherwise(col("_ts"))
      )
      
      # Step 3: Add missing ":00" seconds to timestamps
      df = df.withColumn(
          "_ts",
          when(
              col("_ts").rlike(r"^\d{1,2}-\d{1,2}-\d{2,4}\s+\d{1,2}:\d{2}$"),
              concat(col("_ts"), lit(":00"))
          ).when(
              col("_ts").rlike(r"^\d{1,2}-\d{1,2}-\d{2,4}\s+\d{1,2}:\d{2}\s(AM|PM)$"),
              concat(col("_ts"), lit(":00"))
          ).otherwise(col("_ts"))
      )
  
      formats = [
          "dd-MM-yyyy HH:mm:ss", "dd-MM-yyyy hh:mm:ss a",
          "dd-MM-yyyy HH:mm", "dd-MM-yyyy hh:mm a",
          "dd-MM-yy HH:mm:ss", "dd-MM-yy hh:mm:ss a",
          "dd-MM-yy HH:mm", "dd-MM-yy hh:mm a",
          "d-M-yyyy HH:mm:ss", "d-M-yyyy hh:mm:ss a",
          "d-M-yyyy HH:mm", "d-M-yyyy hh:mm a",
          "d-M-yy HH:mm:ss", "d-M-yy hh:mm:ss a",
          "d-M-yy HH:mm", "d-M-yy hh:mm a",
          "dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy hh:mm:ss a",
          "dd/MM/yyyy HH:mm", "dd/MM/yyyy hh:mm a",
          "dd/MM/yy HH:mm:ss", "dd/MM/yy hh:mm:ss a",
          "dd/MM/yy HH:mm", "dd/MM/yy hh:mm a",
          "M-d-yyyy HH:mm:ss", "M-d-yyyy hh:mm:ss a",
          "M-d-yyyy HH:mm", "M-d-yyyy hh:mm a",
          "M-d-yy HH:mm:ss", "M-d-yy hh:mm:ss a",
          "M-d-yy HH:mm", "M-d-yy hh:mm a",
          "M/d/yyyy HH:mm:ss", "MM/dd/yyyy HH:mm:ss",
          "M/d/yyyy HH:mm", "MM/dd/yyyy HH:mm",
          "M/d/yyyy hh:mm:ss a", "MM/dd/yyyy hh:mm:ss a",
          "M/d/yyyy hh:mm a", "MM/dd/yyyy hh:mm a",
          "M/d/yy HH:mm:ss", "MM/dd/yy HH:mm:ss",
          "M/d/yy HH:mm", "MM/dd/yy HH:mm",
          "M/d/yy hh:mm:ss a", "MM/dd/yy hh:mm:ss a",
          "M/d/yy hh:mm a", "MM/dd/yy hh:mm a",
          "M/d/yy", "MM/dd/yy", "M/d/yyyy", "MM/dd/yyyy"
      ]
  
      df = df.withColumn(
          "event_ts",
          coalesce(*[to_timestamp(col("_ts"), f) for f in formats])
      )
  
      return df.drop("_ts")
      
    else:
      df = df.withColumn("_raw_ts", regexp_replace(trim(col(ts_col)),r"\s+"," "))
      
      df = df.withColumn("_excel_days",
                         when(col("_raw_ts").rlike(r"^\d+\s+\d*\.\d+$"),
                              split(col("_raw_ts"), r"\s+")[0].cast("double")))
      df = df.withColumn("_excel_fraction",
                         when(col("_excel_days").isNotNull(),
                              split(col("_raw_ts"), r"\s+")[1].cast("double")))
      df = df.withColumn("_excel_value",
                         when(col("_excel_days").isNotNull(), col("_excel_days")+col("_excel_fraction")))
      df = df.withColumn("_excel_ts",
                         when(col("_excel_value").isNotNull(),
                              expr("timestampadd(SECOND,cast((_excel_value-25569)*86400 as int), timestamp('1970-01-01'))")))
      
      formats = [
          "dd-MM-yyyy HH:mm:ss", "dd-MM-yyyy hh:mm:ss a",
          "dd-MM-yyyy HH:mm", "dd-MM-yyyy hh:mm a",
          "dd-MM-yy HH:mm:ss", "dd-MM-yy hh:mm:ss a",
          "dd-MM-yy HH:mm", "dd-MM-yy hh:mm a",
          "d-M-yyyy HH:mm:ss", "d-M-yyyy hh:mm:ss a",
          "d-M-yyyy HH:mm", "d-M-yyyy hh:mm a",
          "d-M-yy HH:mm:ss", "d-M-yy hh:mm:ss a",
          "d-M-yy HH:mm", "d-M-yy hh:mm a",
          "dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy hh:mm:ss a",
          "dd/MM/yyyy HH:mm", "dd/MM/yyyy hh:mm a",
          "dd/MM/yy HH:mm:ss", "dd/MM/yy hh:mm:ss a",
          "dd/MM/yy HH:mm", "dd/MM/yy hh:mm a",
          "M-d-yyyy HH:mm:ss", "M-d-yyyy hh:mm:ss a",
          "M-d-yyyy HH:mm", "M-d-yyyy hh:mm a",
          "M-d-yy HH:mm:ss", "M-d-yy hh:mm:ss a",
          "M-d-yy HH:mm", "M-d-yy hh:mm a",
          "M/d/yyyy hh:mm:ss a", "MM/dd/yyyy hh:mm:ss a",
          "M/d/yyyy hh:mm a", "MM/dd/yyyy hh:mm a",
          "M/d/yy hh:mm:ss a", "MM/dd/yy hh:mm:ss a",
          "M/d/yy hh:mm a", "MM/dd/yy hh:mm a",
          "M/d/yy", "MM/dd/yy", "M/d/yyyy", "MM/dd/yyyy"
      ]
  
      df = df.withColumn("_string_ts", coalesce(*[to_timestamp(col("_raw_ts"), f) for f in formats]))
  
      df = df.withColumn("event_ts",
                         when(col("_excel_ts").isNotNull(), col("_excel_ts"))
                         .otherwise(col("_string_ts")))

      return df

# ---------------- SAFE READER ----------------
def safe_read(func, path):
    try:
        if path_exists(path):
            df = func(path)
            if df is not None and not df.rdd.isEmpty():
                return df
        else:
            print(path + " doesn't exist")
    except Exception as e:
        print(f"Error reading {path}: {e}")
    return None

# ---------------- DATA READERS (POSTLOGIN ONLY) ----------------
def read_stacy(path):
    df = spark.read.option("header", True).csv(path)

    # robust timestamp column selection
    possible_ts = ["HKT", "date (UTC)", "Date", "Timestamp", "event_ts"]
    ts_col = None
    for c in possible_ts:
        if c in df.columns:
            ts_col = c
            break
    if ts_col is None:
        raise Exception(f"Stacy: No timestamp col found. Available cols: {df.columns}")

    user_col = "user_id" if "user_id" in df.columns else ("customer_id" if "customer_id" in df.columns else None)
    if user_col is None:
        raise Exception(f"Stacy: No user_id/customer_id col found. Available cols: {df.columns}")

    df = df.filter(col(user_col).isNotNull() & (trim(col(user_col)) != ""))
    df = normalize_timestamp(df, ts_col)

    return df.select(
        col(user_col).alias("user_id"),
        col("event_ts"),
        lit("Stacy").alias("channel")
    )

def read_call(path):
    df = spark.read.option("header", True).csv(path)
    df = df.filter(col("Verification Status") == "Pass")
    df = df.filter(col("Customer No (CTI)").isNotNull() & (trim(col("Customer No (CTI)")) != ""))
    df = normalize_timestamp(df, "Call Start Time")
    return df.select(
        col("Customer No (CTI)").alias("user_id"),
        col("event_ts"),
        lit("Call").alias("channel")
    )

def read_chat(path):
    df = spark.read.option("header", True).csv(path)
    df = df.filter(col("Pre/Post") == "Postlogin")
    df = df.filter(col("REL ID").isNotNull() & (trim(col("REL ID")) != ""))
    df = df.withColumn("_ts", concat_ws(" ", col("Date7"), col("StartTime")))
    df = normalize_timestamp(df, "_ts")
    return df.select(
        col("REL ID").alias("user_id"),
        col("event_ts"),
        lit("Chat").alias("channel")
    )

# ---------------- LOAD DATA ----------------
dfs = []

# Stacy (2 files: en/zh, postlogin)
for cnt in stacy_cnt:
    for auth in stacy_auth:
        p = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_{auth}login.csv"
        d = safe_read(read_stacy, p)
        if d is not None:
            dfs.append(d)

# Call
d = safe_read(read_call, f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv")
if d is not None:
    dfs.append(d)

# Chat
d = safe_read(read_chat, f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv")
if d is not None:
    dfs.append(d)

if not dfs:
    raise ValueError("No data found for this month")

combined_df = reduce(lambda a, b: a.unionByName(b), dfs) \
    .dropna(subset=["user_id", "event_ts"]) \
    .repartition("user_id") \
    .cache()

# ============================================================
# GLOBAL TOTALS (INTERACTION LEVEL)
# ============================================================
total_postlogin_cases = combined_df.count()  # interaction rows
total_distinct_users = combined_df.select("user_id").distinct().count()

# ============================================================
# 1) ORDER INTERACTIONS PER USER (MONTH) => rn
# ============================================================
w_user = Window.partitionBy("user_id").orderBy(col("event_ts").asc())

df = combined_df.withColumn("rn", row_number().over(w_user))

# user_total_interactions = total interaction rows per user (month)
user_totals = df.groupBy("user_id").agg(count("*").alias("user_total_interactions"))

df = df.join(user_totals, "user_id", "inner")

# ============================================================
# 2) STARTER CHANNEL (rn=1)
# ============================================================
starter_df = (
    df.filter(col("rn") == 1)
      .select("user_id", col("channel").alias("starter_channel"))
)

df = df.join(starter_df, "user_id", "inner")

# ============================================================
# 3) TOTAL_CASE (FORCED) = SUM OF INTERACTIONS BY STARTER
#    Ensures sum(total_case across starter channels) == total_postlogin_cases
# ============================================================
total_case_df = (
    df.groupBy("starter_channel")
      .agg(count("*").alias("total_case"),
           countDistinct("user_id").alias("starter_customers"))
)

# ============================================================
# 4) FOLLOW-UP BUCKETS (FORCED, INTERACTION-SUM)
#    Bucket is based on user_total_interactions,
#    and contributes ALL user interactions into ONE bucket.
# ============================================================
user_bucket = (
    user_totals
    .withColumn(
        "bucket",
        when(col("user_total_interactions") == 1, "0")
        .when(col("user_total_interactions") == 2, "1")
        .when(col("user_total_interactions") == 3, "2")
        .otherwise("3+")
    )
)

# attach starter_channel per user
user_starter = starter_df.dropDuplicates(["user_id"])
user_bucket = user_bucket.join(user_starter, "user_id", "inner")

bucket_pivot = (
    user_bucket.groupBy("starter_channel", "bucket")
    .agg(sum("user_total_interactions").alias("bucket_interactions"))
    .groupBy("starter_channel")
    .pivot("bucket", ["0", "1", "2", "3+"])
    .sum("bucket_interactions")
    .fillna(0)
    .withColumnRenamed("0", "follow_up_0")
    .withColumnRenamed("1", "follow_up_1")
    .withColumnRenamed("2", "follow_up_2")
    .withColumnRenamed("3+", "follow_up_3+")
)

# ============================================================
# 5) SECOND + THIRD CHANNEL (EXCEL-STYLE, INTERACTION-SUM)
#    second_channel = channel of rn=2 per user
#    third_channel  = channel of rn=3 per user
#    Then SUM(user_total_interactions) grouped by starter_channel + second/third
#    This guarantees:
#      sum(second counts) == follow_up_1+follow_up_2+follow_up_3+
#      sum(third  counts) == follow_up_2+follow_up_3+
# ============================================================
per_user_channels = (
    df.groupBy("user_id")
      .agg(
          max(when(col("rn") == 2, col("channel"))).alias("second_channel"),
          max(when(col("rn") == 3, col("channel"))).alias("third_channel")
      )
)

user_enriched = (
    user_bucket
    .join(per_user_channels, "user_id", "left")
)

# second channel interactions sum
second_agg = (
    user_enriched
    .filter(col("second_channel").isNotNull())
    .groupBy("starter_channel", "second_channel")
    .agg(sum("user_total_interactions").alias("sec_interactions"))
)

# third channel interactions sum
third_agg = (
    user_enriched
    .filter(col("third_channel").isNotNull())
    .groupBy("starter_channel", "third_channel")
    .agg(sum("user_total_interactions").alias("third_interactions"))
)

def pivot_top4(df_in, ch_col, val_col, prefix):
    w = Window.partitionBy("starter_channel").orderBy(col(val_col).desc(), col(ch_col).asc())
    ranked = df_in.withColumn("r", row_number().over(w)).filter(col("r") <= 4)

    exprs = []
    for i in range(1, 5):
        exprs += [
            max(when(col("r") == i, col(ch_col))).alias(f"{prefix}_chnl_{i}"),
            max(when(col("r") == i, col(val_col))).alias(f"{prefix}_chnl_count_{i}")
        ]
    return ranked.groupBy("starter_channel").agg(*exprs)

sec_pivot = pivot_top4(second_agg, "second_channel", "sec_interactions", "sec")
third_pivot = pivot_top4(third_agg, "third_channel", "third_interactions", "third")

# ============================================================
# 6) FINAL OUTPUT
# ============================================================
final_df = (
    total_case_df
    .join(bucket_pivot, "starter_channel", "left")
    .join(sec_pivot, "starter_channel", "left")
    .join(third_pivot, "starter_channel", "left")
    .fillna(0, subset=["follow_up_0","follow_up_1","follow_up_2","follow_up_3+"])
    .withColumn("rep_rate", (col("follow_up_1") + col("follow_up_2") + col("follow_up_3+")) / col("total_case"))
    .withColumn("Date", lit(part))
    .withColumn("total_postlogin_cases", lit(total_postlogin_cases))
    .withColumn("total_distinct_users", lit(total_distinct_users))
    .withColumnRenamed("starter_channel", "Channel")
)

# --------------------------------------------------
# FINAL COLUMN ORDER (YOUR FORMAT)
# --------------------------------------------------
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

# ---------------- SHOW RESULT ----------------
final_df.show(truncate=False)

# ============================================================
# OPTIONAL VALIDATION PRINTS (EQUALITIES)
# ============================================================
# 1) sum(total_case) across channels == total_postlogin_cases
chk1 = final_df.agg(sum("total_case").alias("sum_total_case")).collect()[0]["sum_total_case"]
print("CHECK sum(total_case) == total_postlogin_cases :", chk1, "==", total_postlogin_cases)

# 2) for each row: followup sum == total_case
final_df.select(
    "Channel", "total_case",
    (col("follow_up_0")+col("follow_up_1")+col("follow_up_2")+col("follow_up_3+")).alias("followup_sum")
).show(truncate=False)
