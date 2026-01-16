from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType
from functools import reduce

# ============================================================
# SPARK SESSION (Hue)
# ============================================================
spark = SparkSession.builder.appName("PostLoginFlow_Timestamp_InteractionLevel").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ============================================================
# CONFIG
# ============================================================
mnth = "oct"
part = "202510"

stacy_cnt = ["en", "zh"]
stacy_auth = ["post"]  # files: {mnth}_stacy_en_postlogin.csv and {mnth}_stacy_zh_postlogin.csv

# NOTE: IVR REMOVED as you asked
CHANNEL_ORDER = ["Stacy", "Chat", "Call"]

# ============================================================
# HDFS HELPERS
# ============================================================
hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

def path_exists(p):
    return hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p))

# ============================================================
# TIMESTAMP NORMALIZER (your working version)
# ============================================================
def normalize_timestamp(df, ts_col):
    if ts_col == "Call Start Time":
        df = df.withColumn("_ts", regexp_replace(trim(col(ts_col)), r"\s+", " "))

        # Step 1: Handle "45231 0.5625" -> "45231.5625"
        df = df.withColumn(
            "_ts",
            when(
                col("_ts").rlike(r"^\d+\s+\d+(\.\d+)?$"),
                regexp_replace(col("_ts"), r"(\d+)\s+(\d+(\.\d+)?)$", r"\1.\2")
            ).otherwise(col("_ts"))
        )

        # Step 2: Convert Excel numeric dates to timestamp
        df = df.withColumn(
            "_ts",
            when(
                col("_ts").rlike(r"^\d+(\.\d+)?$"),
                from_unixtime(unix_timestamp(lit("1899-12-30")) + (col("_ts").cast("double") * 86400))
            ).otherwise(col("_ts"))
        )

        # Step 3: Add missing seconds
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

        df = df.withColumn("event_ts", coalesce(*[to_timestamp(col("_ts"), f) for f in formats]))
        return df.drop("_ts")

    else:
        df = df.withColumn("_raw_ts", regexp_replace(trim(col(ts_col)), r"\s+", " "))

        df = df.withColumn(
            "_excel_days",
            when(col("_raw_ts").rlike(r"^\d+\s+\d*\.\d+$"), split(col("_raw_ts"), r"\s+")[0].cast("double"))
        )
        df = df.withColumn(
            "_excel_fraction",
            when(col("_excel_days").isNotNull(), split(col("_raw_ts"), r"\s+")[1].cast("double"))
        )
        df = df.withColumn(
            "_excel_value",
            when(col("_excel_days").isNotNull(), col("_excel_days") + col("_excel_fraction"))
        )
        df = df.withColumn(
            "_excel_ts",
            when(
                col("_excel_value").isNotNull(),
                expr("timestampadd(SECOND,cast((_excel_value-25569)*86400 as int), timestamp('1970-01-01'))")
            )
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

            "M/d/yyyy hh:mm:ss a", "MM/dd/yyyy hh:mm:ss a",
            "M/d/yyyy hh:mm a", "MM/dd/yyyy hh:mm a",
            "M/d/yy hh:mm:ss a", "MM/dd/yy hh:mm:ss a",
            "M/d/yy hh:mm a", "MM/dd/yy hh:mm a",

            "M/d/yy", "MM/dd/yy", "M/d/yyyy", "MM/dd/yyyy"
        ]

        df = df.withColumn("_string_ts", coalesce(*[to_timestamp(col("_raw_ts"), f) for f in formats]))
        df = df.withColumn("event_ts", when(col("_excel_ts").isNotNull(), col("_excel_ts")).otherwise(col("_string_ts")))
        return df

# ============================================================
# SAFE READ (WITH DEBUG)
# ============================================================
def safe_read(tag, func, path, status_rows):
    try:
        if not path_exists(path):
            status_rows.append((tag, path, "Y", "N", "exists_check", "path_missing", None, None))
            print(f"[MISSING] {tag}: {path}")
            return None

        df = func(path)
        if df is None:
            status_rows.append((tag, path, "Y", "N", "read", "returned_none", None, None))
            print(f"[FAIL] {tag}: read returned None: {path}")
            return None

        # light action for debugging (still ok in Hue)
        cnt = df.count()
        if cnt == 0:
            status_rows.append((tag, path, "Y", "N", "count", "empty_df", 0, None))
            print(f"[EMPTY] {tag}: {path}")
            return None

        status_rows.append((tag, path, "Y", "Y", "ok", "read_ok", int(cnt), None))
        print(f"[OK] {tag}: {path}  rows={cnt}")
        return df

    except Exception as e:
        status_rows.append((tag, path, "Y", "N", "exception", str(e)[:500], None, None))
        print(f"[ERROR] {tag}: {path}  -> {e}")
        return None

# ============================================================
# READERS (return user_id, event_ts, channel)
# ============================================================
def read_stacy(path):
    df = spark.read.option("header", True).csv(path)
    user_col = "user_id" if "user_id" in df.columns else "customer_id"
    ts_col = "HKT" if "HKT" in df.columns else "date (UTC)"
    df = df.filter(col(user_col).isNotNull() & (trim(col(user_col)) != ""))
    df = normalize_timestamp(df, ts_col)
    return df.select(col(user_col).alias("user_id"), col("event_ts"), lit("Stacy").alias("channel"))

def read_chat(path):
    df = spark.read.option("header", True).csv(path)
    df = df.filter(col("Pre/Post") == "Postlogin")
    df = df.filter(col("REL ID").isNotNull() & (trim(col("REL ID")) != ""))
    df = df.withColumn("_ts", concat_ws(" ", col("Date7"), col("StartTime")))
    df = normalize_timestamp(df, "_ts")
    return df.select(col("REL ID").alias("user_id"), col("event_ts"), lit("Chat").alias("channel"))

def read_call(path):
    df = spark.read.option("header", True).csv(path)
    df = df.filter(col("Verification Status") == "Pass")
    df = df.filter(col("Customer No (CTI)").isNotNull() & (trim(col("Customer No (CTI)")) != ""))
    df = normalize_timestamp(df, "Call Start Time")
    return df.select(col("Customer No (CTI)").alias("user_id"), col("event_ts"), lit("Call").alias("channel"))

# ============================================================
# LOAD DATA (NO IVR)
# ============================================================
status_rows = []
dfs = []

for cnt in stacy_cnt:
    for auth in stacy_auth:
        p = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_{auth}login.csv"
        d = safe_read(f"stacy_{cnt}_{auth}", read_stacy, p, status_rows)
        if d is not None:
            dfs.append(d)

d = safe_read("chat", read_chat, f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv", status_rows)
if d is not None:
    dfs.append(d)

d = safe_read("call", read_call, f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv", status_rows)
if d is not None:
    dfs.append(d)

# ---- DEBUG STATUS DF (schema to avoid ValueError type inference) ----
status_schema = StructType([
    StructField("dataset", StringType(), True),
    StructField("path", StringType(), True),
    StructField("exists", StringType(), True),
    StructField("ok", StringType(), True),
    StructField("stage", StringType(), True),
    StructField("reason", StringType(), True),
    StructField("row_cnt", LongType(), True),
    StructField("null_event_ts_cnt", LongType(), True),
])
status_df = spark.createDataFrame(status_rows if len(status_rows) else [], schema=status_schema)
print("========== READ STATUS ==========")
status_df.show(truncate=False)

if not dfs:
    raise ValueError("No data found after reading (check status_df above)")

combined_df = reduce(lambda a, b: a.unionByName(b), dfs)

# Drop bad timestamps but keep debug
null_ts_cnt = combined_df.filter(col("event_ts").isNull()).count()
print(f"NULL event_ts rows before drop: {null_ts_cnt}")

combined_df = combined_df.dropna(subset=["user_id", "event_ts"]).cache()

print("========== COMBINED DF CHECK ==========")
print("combined_df rows:", combined_df.count())
print("combined_df distinct users:", combined_df.select("user_id").distinct().count())
combined_df.groupBy("channel").count().show()

# ============================================================
# INTERACTION-LEVEL (ENTITY): 1 row = 1 interaction/contact
# We will FORCE stakeholder equalities by distributing interactions by starter_channel (first-ever channel per user in month)
# ============================================================
total_postlogin_cases = combined_df.count()
total_distinct_users = combined_df.select("user_id").distinct().count()

# Order interactions per user by event_ts, tie-break by channel stable order
channel_sort = when(col("channel") == "Stacy", lit(1)).when(col("channel") == "Chat", lit(2)).otherwise(lit(3))

w_user = Window.partitionBy("user_id").orderBy(col("event_ts").asc(), channel_sort.asc())

df = (combined_df
      .withColumn("rn", row_number().over(w_user))
      .cache())

starter_df = (df.filter(col("rn") == 1)
              .select("user_id", col("channel").alias("starter_channel")))

df = df.join(starter_df, "user_id", "inner")

# ============================================================
# FOLLOW-UP BUCKETS (FORCED, INTERACTION-LEVEL)
# Definition:
# - For each user, total interactions in the month = N
# - We assign ALL N interactions to starter_channel’s totals (forced stakeholder view)
# - Bucket meaning:
#     follow_up_0 counts interactions from users with N=1
#     follow_up_1 counts interactions from users with N=2
#     follow_up_2 counts interactions from users with N=3
#     follow_up_3+ counts interactions from users with N>=4
# This guarantees:
#   follow_up_0+1+2+3+ == total_case
#   sum(total_case across starter_channel) == total_postlogin_cases
# ============================================================
user_cnt = df.groupBy("user_id").agg(count("*").alias("user_total_contacts"))
df = df.join(user_cnt, "user_id")

df = df.withColumn(
    "bucket",
    when(col("user_total_contacts") == 1, "0")
    .when(col("user_total_contacts") == 2, "1")
    .when(col("user_total_contacts") == 3, "2")
    .otherwise("3+")
)

total_case_df = df.groupBy("starter_channel").agg(count("*").alias("total_case"))

starter_customers_df = df.groupBy("starter_channel").agg(countDistinct("user_id").alias("starter_customers"))

bucket_pivot = (
    df.groupBy("starter_channel", "bucket").count()
      .groupBy("starter_channel")
      .pivot("bucket", ["0", "1", "2", "3+"])
      .sum("count")
      .fillna(0)
      .withColumnRenamed("0", "follow_up_0")
      .withColumnRenamed("1", "follow_up_1")
      .withColumnRenamed("2", "follow_up_2")
      .withColumnRenamed("3+", "follow_up_3+")
)

# rep_rate forced as you requested:
# (follow_up_1 + follow_up_2 + follow_up_3+) / total_case
rep_rate_df = (
    total_case_df.join(bucket_pivot, "starter_channel", "inner")
    .withColumn("rep_rate", (col("follow_up_1") + col("follow_up_2") + col("follow_up_3+")) / col("total_case"))
)

# ============================================================
# 2ND / 3RD CHANNEL (FORCED, INTERACTION-LEVEL)
# Your excel logic says:
#   For starter_channel S:
#     second-channel counts should sum to (follow_up_1 + follow_up_2 + follow_up_3+)
#     third-channel counts should sum to (follow_up_2 + follow_up_3+)
#
# We achieve this by:
# - For each user: find their 2nd interaction channel (rn=2), 3rd interaction channel (rn=3)
# - Weight by ALL interactions of that user (user_total_contacts) and attribute to starter_channel
#   (this matches your example where a1 contributes 133, not 1)
# - For "second-channel total": include users with N>=2 (i.e. bucket in {1,2,3+})
# - For "third-channel total": include users with N>=3 (i.e. bucket in {2,3+})
# ============================================================
nth = df.select("user_id", "starter_channel", "rn", "channel", "user_total_contacts")

sec_base = (
    nth.filter(col("rn") == 2)
       .groupBy("starter_channel", col("channel").alias("sec_channel"))
       .agg(sum(col("user_total_contacts")).alias("sec_weight"))
)

third_base = (
    nth.filter(col("rn") == 3)
       .groupBy("starter_channel", col("channel").alias("third_channel"))
       .agg(sum(col("user_total_contacts")).alias("third_weight"))
)

def pivot_rank_weight(base_df, chan_col, weight_col, prefix):
    w = Window.partitionBy("starter_channel").orderBy(col(weight_col).desc(), col(chan_col).asc())
    ranked = base_df.withColumn("r", row_number().over(w)).filter(col("r") <= 4)

    exprs = []
    for i in range(1, 5):
        exprs += [
            max(when(col("r") == i, col(chan_col))).alias(f"{prefix}_chnl_{i}"),
            max(when(col("r") == i, col(weight_col))).alias(f"{prefix}_chnl_count_{i}")
        ]
    return ranked.groupBy("starter_channel").agg(*exprs)

sec_pivot = pivot_rank_weight(sec_base, "sec_channel", "sec_weight", "sec")
third_pivot = pivot_rank_weight(third_base, "third_channel", "third_weight", "third")

# ============================================================
# FINAL OUTPUT
# ============================================================
final_df = (
    rep_rate_df
    .join(starter_customers_df, "starter_channel", "inner")
    .join(sec_pivot, "starter_channel", "left")
    .join(third_pivot, "starter_channel", "left")
    .withColumn("Date", lit(part))
    .withColumn("total_postlogin_cases", lit(total_postlogin_cases))
    .withColumn("total_distinct_users", lit(total_distinct_users))
    .withColumnRenamed("starter_channel", "Channel")
)

# Required order (your format)
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

# Sort output
final_df = final_df.orderBy(
    when(col("Channel") == "Stacy", lit(1))
    .when(col("Channel") == "Chat", lit(2))
    .otherwise(lit(3))
)

print("========== FINAL RESULT ==========")
final_df.show(truncate=False)

# ============================================================
# DEBUG: EQUALITY CHECKS (Hue prints)
# ============================================================
print("========== EQUALITY CHECKS ==========")

# 1) per channel: followup sum == total_case
chk1 = final_df.select(
    "Channel",
    "total_case",
    (col("follow_up_0") + col("follow_up_1") + col("follow_up_2") + col("follow_up_3+")).alias("follow_sum")
)
chk1.show(truncate=False)

# 2) across channels: sum(total_case) == total_postlogin_cases
sum_total_case = final_df.agg(sum("total_case").alias("sum_total_case")).collect()[0]["sum_total_case"]
print("sum(total_case across Channel) =", sum_total_case)
print("total_postlogin_cases         =", total_postlogin_cases)

# 3) across channels: sum(starter_customers) == total_distinct_users (each user has exactly one starter_channel)
sum_starter_customers = final_df.agg(sum("starter_customers").alias("sum_starter_customers")).collect()[0]["sum_starter_customers"]
print("sum(starter_customers across Channel) =", sum_starter_customers)
print("total_distinct_users                 =", total_distinct_users)

# 4) second/third forced totals should match follow-up totals:
# second sum == follow_up_1+2+3+
sec_sum_df = final_df.select(
    "Channel",
    (coalesce(col("sec_chnl_count_1"), lit(0)) +
     coalesce(col("sec_chnl_count_2"), lit(0)) +
     coalesce(col("sec_chnl_count_3"), lit(0)) +
     coalesce(col("sec_chnl_count_4"), lit(0))).alias("sec_sum"),
    (col("follow_up_1") + col("follow_up_2") + col("follow_up_3+")).alias("should_be")
)
print("== second channel sum check ==")
sec_sum_df.show(truncate=False)

# third sum == follow_up_2+3+
third_sum_df = final_df.select(
    "Channel",
    (coalesce(col("third_chnl_count_1"), lit(0)) +
     coalesce(col("third_chnl_count_2"), lit(0)) +
     coalesce(col("third_chnl_count_3"), lit(0)) +
     coalesce(col("third_chnl_count_4"), lit(0))).alias("third_sum"),
    (col("follow_up_2") + col("follow_up_3+")).alias("should_be")
)
print("== third channel sum check ==")
third_sum_df.show(truncate=False)
