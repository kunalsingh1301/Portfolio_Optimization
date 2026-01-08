from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

# ---------------- SPARK SESSION ----------------
spark = SparkSession.builder.appName("xxxx").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ---------------- CONFIG ----------------
month_part_mapping = {
    "jan": "202501",
    "feb": "202502",
    "mar": "202503",
    "apr": "202504",
    "may": "202505",
    "jun": "202506",
    "jul": "202507",
    "aug": "202508",
    "sep": "202509",
    "oct": "202510",
    "nov": "202511",
}

stacy_cnt = ["en", "zh"]
stacy_auth = ["post"]
post_login_values = ["OTP|S", "VB|S", "TPIN|S"]

# ---------------- HDFS HELPERS ----------------
hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
def path_exists(p):
    return hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p))

# ---------------- NORMALIZE TIMESTAMP ----------------
def normalize_timestamp(df, ts_col):
    # Use the full normalize_timestamp code from your last working version
    # including Excel handling, dash/slash dates, AM/PM, etc.
    # ...
    # (paste your latest normalize_timestamp function here)
    return df  # placeholder

# ---------------- DATA READERS ----------------
def read_stacy(path):
    df = spark.read.option("header", True).csv(path)
    ts_col = "HKT" if "HKT" in df.columns else "date (UTC)"
    user_col = "user_id" if "user_id" in df.columns else "customer_id"
    df = normalize_timestamp(df, ts_col)
    return df.select(col(user_col).alias("user_id"), "event_ts", lit("Stacy").alias("channel"))

def read_ivr(path):
    df = spark.read.option("header", True).csv(path)
    df = df.filter(col("ONE_FA").isin(post_login_values))
    df = normalize_timestamp(df, "STARTTIME")
    return df.select(col("REL_ID").alias("user_id"), "event_ts", lit("IVR").alias("channel"))

def read_call(path):
    df = spark.read.option("header", True).csv(path)
    df = normalize_timestamp(df, "Call Start Time")
    return df.select(col("Customer No (CTI)").alias("user_id"), "event_ts", lit("Call").alias("channel"))

def read_chat(path):
    df = spark.read.option("header", True).csv(path)
    df = df.filter(col("Pre/Post") == "Postlogin")
    df = df.withColumn("_ts", concat_ws(" ", col("Date7"), col("StartTime")))
    df = normalize_timestamp(df, "_ts")
    return df.select(col("REL ID").alias("user_id"), "event_ts", lit("Chat").alias("channel"))

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

# ---------------- RUN PIPELINE FOR ALL MONTHS ----------------
all_months_df = []

for mnth, part in month_part_mapping.items():
    print(f"\n=== Processing {mnth.upper()} ({part}) ===")
    dfs = []

    # Stacy
    for cnt in stacy_cnt:
        for auth in stacy_auth:
            p = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_{auth}login.csv"
            df = safe_read(read_stacy, p)
            if df: dfs.append(df)

    # IVR
    for i in range(1, 5):
        p = f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv"
        df = safe_read(read_ivr, p)
        if df: dfs.append(df)

    # Call
    p = f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"
    df = safe_read(read_call, p)
    if df: dfs.append(df)

    # Chat
    p = f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv"
    df = safe_read(read_chat, p)
    if df: dfs.append(df)

    if not dfs:
        print(f"No data found for {mnth}")
        continue

    combined_df = reduce(lambda a, b: a.unionByName(b), dfs) \
        .dropna(subset=["user_id", "event_ts"]) \
        .repartition("user_id") \
        .cache()

    # ---------------- RN + STARTER CHANNEL ----------------
    w = Window.partitionBy("user_id").orderBy("event_ts")
    df_rn = combined_df.withColumn("rn", row_number().over(w))
    starter_df = df_rn.filter(col("rn") == 1).select("user_id", col("channel").alias("starter_channel"))
    df_rn = df_rn.join(starter_df, "user_id")

    # ---------------- TOTAL + REP RATE ----------------
    agg_df = df_rn.groupBy("starter_channel").agg(
        count("*").alias("total_case"),
        countDistinct("user_id").alias("uniq_cust")
    ).withColumn("rep_rate", (col("total_case") - col("uniq_cust")) * 100 / col("total_case"))

    # ---------------- FOLLOW-UP BUCKETS ----------------
    follow_df = df_rn.groupBy("starter_channel", "user_id").count()
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
        .pivot("bucket", ["0","1","2","3+"])
        .sum("count")
        .fillna(0)
        .withColumnRenamed("0","follow_up_0")
        .withColumnRenamed("1","follow_up_1")
        .withColumnRenamed("2","follow_up_2")
        .withColumnRenamed("3+","follow_up_3+")
    )

    # ---------------- TOP 2ND / 3RD CONTACT ----------------
    def top_contact(df, rn, prefix, top_n=4):
        base = df.filter(col("rn") == rn).groupBy("starter_channel", "channel").count()
        w = Window.partitionBy("starter_channel").orderBy(col("count").desc())
        ranked = base.withColumn("rank", row_number().over(w)).filter(col("rank") <= top_n)
        exprs = []
        for i in range(1, top_n+1):
            exprs += [
                max(when(col("rank") == i, col("channel"))).alias(f"{prefix}_chnl_{i}"),
                max(when(col("rank") == i, col("count"))).alias(f"{prefix}_chnl_count_{i}")
            ]
        return ranked.groupBy("starter_channel").agg(*exprs)

    sec_df = top_contact(df_rn, 2, "sec")
    third_df = top_contact(df_rn, 3, "third")

    # ---------------- FINAL JOIN ----------------
    final_df = (
        agg_df
        .join(bucket_pivot, "starter_channel", "left")
        .join(sec_df, "starter_channel", "left")
        .join(third_df, "starter_channel", "left")
        .withColumnRenamed("starter_channel","Channel")
        .withColumn("Date", lit(part))
    )

    # ---------------- FIX SCHEMA ----------------
    columns = [
        "Date","Channel","total_case","uniq_cust","rep_rate",
        "follow_up_0","follow_up_1","follow_up_2","follow_up_3+",
        "sec_chnl_1","sec_chnl_2","sec_chnl_3","sec_chnl_4",
        "sec_chnl_count_1","sec_chnl_count_2","sec_chnl_count_3","sec_chnl_count_4",
        "third_chnl_1","third_chnl_2","third_chnl_3","third_chnl_4",
        "third_chnl_count_1","third_chnl_count_2","third_chnl_count_3","third_chnl_count_4"
    ]
    for c in columns:
        if c not in final_df.columns:
            final_df = final_df.withColumn(c, lit(None))
    final_df = final_df.select(columns)

    all_months_df.append(final_df)

# ---------------- UNION ALL MONTHS ----------------
if all_months_df:
    result_df = reduce(lambda a,b: a.unionByName(b), all_months_df)
    result_df.show(truncate=False)
else:
    print("No data found for any month.")
