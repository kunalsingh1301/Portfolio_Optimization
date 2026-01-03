from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ---------------- SPARK SESSION ----------------
spark = SparkSession.builder.appName("ChannelFlowNormalized").getOrCreate()

# Use LEGACY parser to avoid Spark 3+ strict timestamp parsing issues
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ---------------- CONFIG ----------------
mnth = "jan"
part = "202501"

stacy_cnt = ["en", "zh"]
stacy_auth = ["post"]
post_login_values = ["OTP|S", "VB|S", "TPIN|S"]
channels = ["Stacy", "IVR", "Call", "Chat"]

hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

def path_exists(p):
    return hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p))

# ---------------- SAFE TIMESTAMP NORMALIZATION ----------------
def normalize_timestamp(df, ts_col):
    """
    Normalize messy timestamps into Spark TimestampType safely.
    Handles:
      - 1 or 2 digit day/month
      - 2 or 4 digit year
      - 24-hour and 12-hour with AM/PM
      - Missing seconds
    """
    # Clean spaces
    df = df.withColumn("_ts_clean", trim(col(ts_col)))

    # Fill missing seconds
    df = df.withColumn("_ts_clean",
                       when(col("_ts_clean").rlike(r"^\d{1,2}-\d{1,2}-\d{2,4} \d{1,2}:\d{2}$"),
                            concat_ws(":", col("_ts_clean"), lit("00")))
                       .otherwise(col("_ts_clean")))

    # Multiple formats
    formats = [
        "d-M-yy HH:mm:ss",
        "d-M-yy hh:mm:ss a",
        "d-M-yy HH:mm",
        "d-M-yy hh:mm a",
        "d-M-yyyy HH:mm:ss",
        "d-M-yyyy hh:mm:ss a",
        "d-M-yyyy HH:mm",
        "d-M-yyyy hh:mm a"
    ]
    exprs = [to_timestamp(col("_ts_clean"), f) for f in formats]
    df = df.withColumn("event_ts", coalesce(*exprs))
    df = df.withColumn("event_ts", col("event_ts").cast("timestamp"))
    return df.drop("_ts_clean")

# ---------------- SAFE READER WRAPPER ----------------
def safe_read(func, path):
    """
    Reads a CSV safely: skips empty/malformed files
    """
    try:
        if path_exists(path):
            df = func(path)
            if df is not None and not df.rdd.isEmpty():
                # Clean column names (strip spaces)
                df = df.toDF(*[c.strip() for c in df.columns])
                return df
    except Exception as e:
        print(f"Skipping {path} due to error: {e}")
    return None

# ---------------- DATA READERS ----------------
def read_stacy(path):
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    ts_col = "HKT" if "HKT" in df.columns else "date (UTC)"
    user_col = "user_id" if "user_id" in df.columns else "customer_id"
    df = normalize_timestamp(df, ts_col)
    return df.select(
        col(user_col).alias("user_id"),
        col("event_ts"),
        lit("Stacy").alias("channel")
    )

def read_ivr(path):
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    df = df.filter(col("ONE_FA").isin(post_login_values))
    df = normalize_timestamp(df, "STARTTIME")
    return df.select(
        col("REL_ID").alias("user_id"),
        col("event_ts"),
        lit("IVR").alias("channel")
    )

def read_call(path):
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    df = normalize_timestamp(df, "Call Start Time")
    return df.select(
        col("Customer No (CTI)").alias("user_id"),
        col("event_ts"),
        lit("Call").alias("channel")
    )

def read_chat(path):
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    df = df.filter(col("Pre/Post") == "Postlogin")
    df = df.withColumn("_ts", concat_ws(" ", col("Date28"), col("StartTime")))
    df = normalize_timestamp(df, "_ts")
    return df.select(
        col("REL ID").alias("user_id"),
        col("event_ts"),
        lit("Chat").alias("channel")
    )

# ---------------- LOAD DATA ----------------
dfs = []

for cnt in stacy_cnt:
    for auth in stacy_auth:
        p = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_{auth}login.csv"
        df = safe_read(read_stacy, p)
        if df:
            dfs.append(df)

for i in range(1, 5):
    p = f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv"
    df = safe_read(read_ivr, p)
    if df:
        dfs.append(df)

call_p = f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"
df = safe_read(read_call, call_p)
if df:
    dfs.append(df)

chat_p = f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv"
df = safe_read(read_chat, chat_p)
if df:
    dfs.append(df)

if not dfs:
    raise ValueError("No valid data files found!")

# ---------------- COMBINE ----------------
combined_df = dfs[0]
for d in dfs[1:]:
    combined_df = combined_df.unionByName(d)

combined_df = combined_df.dropna(subset=["user_id", "event_ts"]).cache()
combined_df.count()  # materialize cache

# ---------------- SEQUENCE ----------------
w = Window.partitionBy("user_id").orderBy("event_ts")
df = combined_df.withColumn("rn", row_number().over(w))

# ---------------- AGGREGATION ----------------
results = []

for ch in channels:
    starters = df.filter((col("rn") == 1) & (col("channel") == ch)) \
                 .select("user_id").distinct()
    scoped = df.join(starters, "user_id")

    agg = scoped.agg(
        count("*").alias("Total_case"),
        countDistinct("user_id").alias("uniq_cust")
    ).collect()[0]

    total_case = agg["Total_case"]
    uniq_cust = agg["uniq_cust"]
    rep_rate = (total_case - uniq_cust) * 100 / total_case if total_case else 0

    bucket = (
        scoped.groupBy("user_id").count()
        .withColumn(
            "bucket",
            when(col("count") == 1, "0")
            .when(col("count") == 2, "1")
            .when(col("count") == 3, "2")
            .otherwise("3+")
        )
        .groupBy("bucket").count()
    )
    bmap = {r["bucket"]: r["count"] for r in bucket.collect()}

    def top_n(n):
        rows = (
            scoped.filter(col("rn") == n)
            .groupBy("channel").count()
            .orderBy(col("count").desc())
            .limit(4).collect()
        )
        chs = [r["channel"] for r in rows] + [""] * (4 - len(rows))
        cnts = [r["count"] for r in rows] + [0] * (4 - len(rows))
        return chs, cnts

    sec_ch, sec_ct = top_n(2)
    thr_ch, thr_ct = top_n(3)

    results.append([
        part, ch, total_case, uniq_cust, rep_rate,
        bmap.get("0", 0), bmap.get("1", 0),
        bmap.get("2", 0), bmap.get("3+", 0),
        *sec_ch, *sec_ct,
        *thr_ch, *thr_ct
    ])

# ---------------- OUTPUT ----------------
columns = [
    "Date", "Channel", "Total_case", "uniq_cust", "rep_rate",
    "follow_up_0", "follow_up_1", "follow_up_2", "follow_up_3+",
    "sec_con_chnl_1", "sec_con_chnl_2", "sec_con_chnl_3", "sec_con_chnl_4",
    "sec_con_chnl_count_1", "sec_con_chnl_count_2", "sec_con_chnl_count_3", "sec_con_chnl_count_4",
    "third_con_chnl_1", "third_con_chnl_2", "third_con_chnl_3", "third_con_chnl_4",
    "third_con_chnl_count_1", "third_con_chnl_count_2", "third_con_chnl_count_3", "third_con_chnl_count_4"
]

output_df = spark.createDataFrame(results, columns)
output_df.show(truncate=False)
