from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

# ---------------- SPARK SESSION ----------------
spark = SparkSession.builder.appName("ChannelFlowNormalized").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ---------------- CONFIG ----------------
mnth = "jan"
part = "202501"

stacy_cnt = ["en", "zh"]
stacy_auth = ["post"]
post_login_values = ["OTP|S", "VB|S", "TPIN|S"]
channels = ["Stacy", "IVR", "Call", "Chat"]

hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

def path_exists(p):
    return hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p))

# ---------------- SAFE TIMESTAMP NORMALIZATION ----------------
def normalize_timestamp(df, ts_col):
    df = df.withColumn("_ts_clean", trim(col(ts_col)))
    df = df.withColumn("_ts_clean",
                       when(col("_ts_clean").rlike(r"^\d{1,2}-\d{1,2}-\d{2,4} \d{1,2}:\d{2}$"),
                            concat_ws(":", col("_ts_clean"), lit("00")))
                       .otherwise(col("_ts_clean")))
    formats = [
        "d-M-yy HH:mm:ss", "d-M-yy hh:mm:ss a", "d-M-yy HH:mm",
        "d-M-yy hh:mm a", "d-M-yyyy HH:mm:ss", "d-M-yyyy hh:mm:ss a",
        "d-M-yyyy HH:mm", "d-M-yyyy hh:mm a"
    ]
    exprs = [to_timestamp(col("_ts_clean"), f) for f in formats]
    df = df.withColumn("event_ts", coalesce(*exprs))
    df = df.withColumn("event_ts", col("event_ts").cast("timestamp"))
    return df.drop("_ts_clean")

# ---------------- SAFE READER ----------------
def safe_read(func, path):
    try:
        if path_exists(path):
            df = func(path)
            if df is not None and not df.rdd.isEmpty():
                df = df.toDF(*[c.strip() for c in df.columns])
                return df
    except:
        return None
    return None

# ---------------- DATA READERS ----------------
def read_stacy(path):
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    ts_col = "HKT" if "HKT" in df.columns else "date (UTC)"
    user_col = "user_id" if "user_id" in df.columns else "customer_id"
    df = normalize_timestamp(df, ts_col)
    return df.select(col(user_col).alias("user_id"), col("event_ts"), lit("Stacy").alias("channel"))

def read_ivr(path):
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    df = df.filter(col("ONE_FA").isin(post_login_values))
    df = normalize_timestamp(df, "STARTTIME")
    return df.select(col("REL_ID").alias("user_id"), col("event_ts"), lit("IVR").alias("channel"))

def read_call(path):
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    df = normalize_timestamp(df, "Call Start Time")
    return df.select(col("Customer No (CTI)").alias("user_id"), col("event_ts"), lit("Call").alias("channel"))

def read_chat(path):
    df = spark.read.option("header", True).option("inferSchema", True).csv(path)
    df = df.filter(col("Pre/Post") == "Postlogin")
    df = df.withColumn("_ts", concat_ws(" ", col("Date28"), col("StartTime")))
    df = normalize_timestamp(df, "_ts")
    return df.select(col("REL ID").alias("user_id"), col("event_ts"), lit("Chat").alias("channel"))

# ---------------- LOAD DATA ----------------
dfs = []

for cnt in stacy_cnt:
    for auth in stacy_auth:
        p = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_{auth}login.csv"
        df = safe_read(read_stacy, p)
        if df: dfs.append(df)

for i in range(1, 5):
    p = f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv"
    df = safe_read(read_ivr, p)
    if df: dfs.append(df)

call_p = f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"
df = safe_read(read_call, call_p)
if df: dfs.append(df)

chat_p = f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv"
df = safe_read(read_chat, chat_p)
if df: dfs.append(df)

if not dfs: raise ValueError("No valid data files found!")

# Union all at once
from functools import reduce
combined_df = reduce(lambda a, b: a.unionByName(b), dfs)

# Drop missing and repartition for window
combined_df = combined_df.dropna(subset=["user_id", "event_ts"]).repartition("user_id").cache()

# ---------------- SEQUENCE ----------------
w = Window.partitionBy("user_id").orderBy("event_ts")
df = combined_df.withColumn("rn", row_number().over(w))

# ---------------- AGGREGATIONS ----------------
# Total case per first channel
first_channel = df.filter(col("rn")==1).groupBy("channel").agg(
    count("*").alias("Total_case"),
    countDistinct("user_id").alias("uniq_cust")
)

# Follow-up bucket per channel
follow_up = df.groupBy("user_id").count().withColumn(
    "bucket",
    when(col("count")==1, "0")
    .when(col("count")==2, "1")
    .when(col("count")==3, "2")
    .otherwise("3+")
)

bucket_agg = follow_up.groupBy("bucket").count().collect()
bmap = {r['bucket']: r['count'] for r in bucket_agg}

# Top 2nd and 3rd channels
# Extract 2nd and 3rd events
events = df.filter(col("rn").isin([2,3]))
events_pivot = events.groupBy("user_id").pivot("rn").agg(first("channel"))

# Count top channels
top2 = events.filter(col("rn")==2).groupBy("channel").count()
top3 = events.filter(col("rn")==3).groupBy("channel").count()

# Assemble final result
results = []

fc_collect = first_channel.collect()
top2_dict = {r['channel']: r['count'] for r in top2.collect()}
top3_dict = {r['channel']: r['count'] for r in top3.collect()}

for r in fc_collect:
    ch = r['channel']
    total_case = r['Total_case']
    uniq_cust = r['uniq_cust']
    rep_rate = (total_case - uniq_cust)*100/total_case if total_case else 0

    # Buckets
    f0 = bmap.get("0",0)
    f1 = bmap.get("1",0)
    f2 = bmap.get("2",0)
    f3 = bmap.get("3+",0)

    # Top 2nd/3rd channels (sorted)
    sec_ch = sorted(top2_dict.items(), key=lambda x: -x[1])
    sec_ch_names = [x[0] for x in sec_ch[:4]] + [""]*(4-len(sec_ch[:4]))
    sec_ch_counts = [x[1] for x in sec_ch[:4]] + [0]*(4-len(sec_ch[:4]))

    thr_ch = sorted(top3_dict.items(), key=lambda x: -x[1])
    thr_ch_names = [x[0] for x in thr_ch[:4]] + [""]*(4-len(thr_ch[:4]))
    thr_ch_counts = [x[1] for x in thr_ch[:4]] + [0]*(4-len(thr_ch[:4]))

    results.append([
        part, ch, total_case, uniq_cust, rep_rate,
        f0,f1,f2,f3,
        *sec_ch_names, *sec_ch_counts,
        *thr_ch_names, *thr_ch_counts
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
