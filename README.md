from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder.appName("PostLoginFlow_Timestamp_Forced_Debug").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ============================================================
# CONFIG
# ============================================================
mnth = "oct"
part = "202510"

stacy_cnt = ["en", "zh"]
stacy_auth = ["post"]

# NOTE: IVR REMOVED as requested

# ============================================================
# HDFS HELPERS
# ============================================================
hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

def path_exists(p):
    return hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p))

# ============================================================
# TIMESTAMP NORMALIZER (YOUR WORKING VERSION)
# ============================================================
def normalize_timestamp(df, ts_col):
    if ts_col == "Call Start Time":
        df = df.withColumn("_ts", regexp_replace(trim(col(ts_col)), r"\s+", " "))

        # Step 1: Handle space-separated numeric Excel values (e.g., "45231 0.5625")
        df = df.withColumn(
            "_ts",
            when(
                col("_ts").rlike(r"^\d+\s+\d+(\.\d+)?$"),
                regexp_replace(col("_ts"), r"(\d+)\s+(\d+(\.\d+)?)$", r"\1.\2"),
            ).otherwise(col("_ts")),
        )

        # Step 2: Convert Excel numeric dates to timestamp (e.g., "45231.5625")
        df = df.withColumn(
            "_ts",
            when(
                col("_ts").rlike(r"^\d+(\.\d+)?$"),
                from_unixtime(unix_timestamp(lit("1899-12-30")) + (col("_ts").cast("double") * 86400)),
            ).otherwise(col("_ts")),
        )

        # Step 3: Add missing ":00" seconds to timestamps
        df = df.withColumn(
            "_ts",
            when(
                col("_ts").rlike(r"^\d{1,2}-\d{1,2}-\d{2,4}\s+\d{1,2}:\d{2}$"),
                concat(col("_ts"), lit(":00")),
            )
            .when(
                col("_ts").rlike(r"^\d{1,2}-\d{1,2}-\d{2,4}\s+\d{1,2}:\d{2}\s(AM|PM)$"),
                concat(col("_ts"), lit(":00")),
            )
            .otherwise(col("_ts")),
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

            "M/d/yy", "MM/dd/yy", "M/d/yyyy", "MM/dd/yyyy",
        ]

        df = df.withColumn("event_ts", coalesce(*[to_timestamp(col("_ts"), f) for f in formats]))
        return df.drop("_ts")

    else:
        df = df.withColumn("_raw_ts", regexp_replace(trim(col(ts_col)), r"\s+", " "))

        df = df.withColumn(
            "_excel_days",
            when(col("_raw_ts").rlike(r"^\d+\s+\d*\.\d+$"), split(col("_raw_ts"), r"\s+")[0].cast("double")),
        )
        df = df.withColumn(
            "_excel_fraction",
            when(col("_excel_days").isNotNull(), split(col("_raw_ts"), r"\s+")[1].cast("double")),
        )
        df = df.withColumn(
            "_excel_value",
            when(col("_excel_days").isNotNull(), col("_excel_days") + col("_excel_fraction")),
        )
        df = df.withColumn(
            "_excel_ts",
            when(
                col("_excel_value").isNotNull(),
                expr(
                    "timestampadd(SECOND,cast((_excel_value-25569)*86400 as int), timestamp('1970-01-01'))"
                ),
            ),
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
            "dd/MM/yyyy attaching", "dd/MM/yyyy hh:mm a",  # harmless if never matches
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

            "M/d/yy", "MM/dd/yy", "M/d/yyyy", "MM/dd/yyyy",
        ]

        df = df.withColumn("_string_ts", coalesce(*[to_timestamp(col("_raw_ts"), f) for f in formats]))
        df = df.withColumn("event_ts", when(col("_excel_ts").isNotNull(), col("_excel_ts")).otherwise(col("_string_ts")))
        return df

# ============================================================
# DEBUGGING: STATUS COLLECTION (Hue-safe)
# ============================================================
status_rows = []

def add_status(dataset, path, exists, ok, stage, reason, cnt=None, null_ts_cnt=None):
    status_rows.append(
        Row(
            dataset=dataset,
            path=path,
            exists=str(exists),
            ok=str(ok),
            stage=stage,
            reason=(reason or "")[:250],
            row_cnt=int(cnt) if cnt is not None else None,
            null_event_ts_cnt=int(null_ts_cnt) if null_ts_cnt is not None else None,
        )
    )

def safe_read(dataset_name, func, path):
    try:
        exists = path_exists(path)
        if not exists:
            add_status(dataset_name, path, exists, 0, "PATH", "PATH_NOT_FOUND")
            return None

        df = spark.read.option("header", True).csv(path)
        add_status(dataset_name, path, exists, 1, "READ", "READ_OK", cnt=df.count())

        out = func(df)  # apply logic/filters/ts
        if out is None:
            add_status(dataset_name, path, exists, 0, "TRANSFORM", "RETURNED_NONE")
            return None

        # quick checks
        sample = out.limit(1).count()
        if sample == 0:
            add_status(dataset_name, path, exists, 0, "TRANSFORM", "EMPTY_AFTER_FILTERS")
            return None

        null_ts = out.filter(col("event_ts").isNull()).count()
        total = out.count()
        add_status(dataset_name, path, exists, 1, "FINAL", "OK", cnt=total, null_ts_cnt=null_ts)

        return out

    except Exception as e:
        add_status(dataset_name, path, True, 0, "EXCEPTION", str(e))
        return None

# ============================================================
# READERS (timestamp-based)
# ============================================================
def stacy_transform(df):
    user_col = "user_id" if "user_id" in df.columns else ("customer_id" if "customer_id" in df.columns else None)
    ts_col = "HKT" if "HKT" in df.columns else ("date (UTC)" if "date (UTC)" in df.columns else None)
    if user_col is None or ts_col is None:
        return None

    df = df.filter(col(user_col).isNotNull() & (trim(col(user_col)) != ""))
    df = normalize_timestamp(df, ts_col)
    return df.select(col(user_col).alias("user_id"), "event_ts", lit("Stacy").alias("channel"))

def chat_transform(df):
    if "REL ID" not in df.columns:
        return None
    df = df.filter(col("Pre/Post") == "Postlogin")
    df = df.filter(col("REL ID").isNotNull() & (trim(col("REL ID")) != ""))
    df = df.withColumn("_ts", concat_ws(" ", col("Date7"), col("StartTime")))
    df = normalize_timestamp(df, "_ts")
    return df.select(col("REL ID").alias("user_id"), "event_ts", lit("Chat").alias("channel"))

def call_transform(df):
    if "Customer No (CTI)" not in df.columns or "Call Start Time" not in df.columns:
        return None
    df = df.filter(col("Customer No (CTI)").isNotNull() & (trim(col("Customer No (CTI)")) != ""))
    df = df.filter(col("Verification Status") == "Pass")
    df = normalize_timestamp(df, "Call Start Time")
    return df.select(col("Customer No (CTI)").alias("user_id"), "event_ts", lit("Call").alias("channel"))

# ============================================================
# LOAD DATA (IVR REMOVED)
# ============================================================
dfs = []

# Stacy: two files en/zh
for cnt in stacy_cnt:
    for auth in stacy_auth:
        p = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_{auth}login.csv"
        df = safe_read(f"Stacy_{cnt}_{auth}", stacy_transform, p)
        if df is not None:
            dfs.append(df)

# Call
p_call = f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"
df = safe_read("Call", call_transform, p_call)
if df is not None:
    dfs.append(df)

# Chat
p_chat = f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv"
df = safe_read("Chat", chat_transform, p_chat)
if df is not None:
    dfs.append(df)

# Show status in Hue
status_df = spark.createDataFrame(status_rows)
status_df.show(truncate=False)

# Also write status to HDFS (guaranteed visibility)
status_out = f"/user/2030435/CallCentreAnalystics/_debug_status/{mnth}_{part}"
status_df.coalesce(1).write.mode("overwrite").option("header", True).csv(status_out)

if not dfs:
    raise ValueError("No data found AFTER filtering/ts parsing. Check /_debug_status output above.")

# ============================================================
# PREP combined_df (your working pattern)
# ============================================================
combined_df = (
    reduce(lambda a, b: a.unionByName(b), dfs)
    .dropna(subset=["user_id", "event_ts"])
    .repartition("user_id")
    .cache()
)

total_postlogin_cases = combined_df.count()
total_distinct_users = combined_df.select(col("user_id")).distinct().count()

# ============================================================
# INTERACTION-LEVEL DEFINITIONS (FORCED STAKEHOLDER LOGIC)
# ------------------------------------------------------------
# - Base entity = interaction row (each row in combined_df)
# - For a given "starter channel" X:
#   * We look at USERS whose FIRST interaction (by event_ts) is in X.
#   * For each such user, let n = total interactions across all channels in month
#   * follow_up_0/1/2/3+ = SUM of n for users in that bucket (not distinct users)
#     (this matches your Excel "sum of Count of use")
#   * 2nd channel counts = SUM of n for users where 2nd interaction channel = Y
#   * 3rd channel counts = SUM of n for users where 3rd interaction channel = Y
# This forces:
#   sum(second_channel_counts) == follow_up_1 + follow_up_2 + follow_up_3+
#   sum(third_channel_counts)  == follow_up_2 + follow_up_3+
#   follow_up_0 + follow_up_1 + follow_up_2 + follow_up_3+ == total_case (forced)
# ============================================================

# order interactions per user
w = Window.partitionBy("user_id").orderBy("event_ts")
ordered = combined_df.withColumn("rn", row_number().over(w))

# user-level facts: first/second/third channel + total interactions n
user_seq = (
    ordered.groupBy("user_id")
    .agg(
        max(when(col("rn") == 1, col("channel"))).alias("starter_channel"),
        max(when(col("rn") == 2, col("channel"))).alias("second_channel"),
        max(when(col("rn") == 3, col("channel"))).alias("third_channel"),
        count("*").alias("n_interactions"),
    )
)

# bucket based on n
user_seq = user_seq.withColumn(
    "bucket",
    when(col("n_interactions") == 1, "0")
    .when(col("n_interactions") == 2, "1")
    .when(col("n_interactions") == 3, "2")
    .otherwise("3+")
)

# starter_customers (true distinct customers)
starter_customers_df = user_seq.groupBy("starter_channel").agg(countDistinct("user_id").alias("starter_customers"))

# follow-ups (FORCED: sum of n_interactions by bucket)
followup_df = (
    user_seq.groupBy("starter_channel", "bucket")
    .agg(sum("n_interactions").alias("bucket_interactions"))
    .groupBy("starter_channel")
    .pivot("bucket", ["0", "1", "2", "3+"])
    .sum("bucket_interactions")
    .fillna(0)
    .withColumnRenamed("0", "follow_up_0")
    .withColumnRenamed("1", "follow_up_1")
    .withColumnRenamed("2", "follow_up_2")
    .withColumnRenamed("3+", "follow_up_3+")
)

# total_case (FORCED: sum of followups)
total_case_df = followup_df.select(
    "starter_channel",
    (col("follow_up_0") + col("follow_up_1") + col("follow_up_2") + col("follow_up_3+")).alias("total_case")
)

# rep_rate (FORCED per your last request)
rep_df = total_case_df.join(followup_df, "starter_channel").withColumn(
    "rep_rate",
    when(col("total_case") == 0, lit(0.0)).otherwise(
        (col("follow_up_1") + col("follow_up_2") + col("follow_up_3+")) / col("total_case")
    )
)

# ============================================================
# SECOND / THIRD CHANNEL (FORCED interaction-sum logic)
# ============================================================
# second: users with n>=2 -> sum n_interactions by second_channel
second_base = (
    user_seq.filter(col("n_interactions") >= 2)
    .groupBy("starter_channel", "second_channel")
    .agg(sum("n_interactions").alias("sec_sum"))
)

# third: users with n>=3 -> sum n_interactions by third_channel
third_base = (
    user_seq.filter(col("n_interactions") >= 3)
    .groupBy("starter_channel", "third_channel")
    .agg(sum("n_interactions").alias("third_sum"))
)

def topN_pivot(df_in, grp_col, val_col, prefix):
    # rank within each starter_channel
    w2 = Window.partitionBy("starter_channel").orderBy(col(val_col).desc(), col(grp_col).asc())
    ranked = df_in.withColumn("r", row_number().over(w2))

    exprs = []
    for i in range(1, 5):
        exprs += [
            max(when(col("r") == i, col(grp_col))).alias(f"{prefix}_chnl_{i}"),
            max(when(col("r") == i, col(val_col))).alias(f"{prefix}_chnl_count_{i}")
        ]
    return ranked.filter(col("r") <= 4).groupBy("starter_channel").agg(*exprs)

sec_pivot = topN_pivot(second_base, "second_channel", "sec_sum", "sec")
third_pivot = topN_pivot(third_base, "third_channel", "third_sum", "third")

# ============================================================
# FINAL ASSEMBLY
# ============================================================
final_df = (
    rep_df
    .join(total_case_df.select("starter_channel", "total_case"), "starter_channel")
    .join(starter_customers_df, "starter_channel", "left")
    .join(sec_pivot, "starter_channel", "left")
    .join(third_pivot, "starter_channel", "left")
    .withColumnRenamed("starter_channel", "Channel")
    .withColumn("Date", lit(part))
    .withColumn("total_postlogin_cases", lit(total_postlogin_cases))
    .withColumn("total_distinct_users", lit(total_distinct_users))
)

# required output order
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

# ============================================================
# DEBUG CHECKS FOR YOUR EQUALITIES (Hue visible)
# ============================================================
check_df = final_df.select(
    "Channel",
    "total_case",
    (col("follow_up_0") + col("follow_up_1") + col("follow_up_2") + col("follow_up_3+")).alias("sum_followups"),
    (col("sec_chnl_count_1") + col("sec_chnl_count_2") + col("sec_chnl_count_3") + col("sec_chnl_count_4")).alias("sum_sec_counts"),
    (col("third_chnl_count_1") + col("third_chnl_count_2") + col("third_chnl_count_3") + col("third_chnl_count_4")).alias("sum_third_counts"),
    (col("follow_up_1") + col("follow_up_2") + col("follow_up_3+")).alias("sum_follow_1p"),
    (col("follow_up_2") + col("follow_up_3+")).alias("sum_follow_2p")
)

check_df.show(truncate=False)

# global equality (forced logic won't necessarily equal raw combined_df count unless stakeholders accept)
global_sum_followups = final_df.select(sum(col("follow_up_0")+col("follow_up_1")+col("follow_up_2")+col("follow_up_3+")).alias("s")).collect()[0]["s"]
spark.createDataFrame([Row(total_postlogin_cases=total_postlogin_cases, sum_followups_all_channels=global_sum_followups)]).show(truncate=False)

# ============================================================
# SHOW RESULT
# ============================================================
final_df.show(truncate=False)

# Optional: write output
out_path = f"/user/2030435/CallCentreAnalystics/_final_flow/{mnth}_{part}"
final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)
