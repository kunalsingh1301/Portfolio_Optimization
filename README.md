from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, row_number, when, count, countDistinct,
    concat_ws, to_timestamp, coalesce, regexp_extract,
    regexp_replace, lpad
)
from pyspark.sql.window import Window

# ---------------- SPARK SESSION ----------------
spark = SparkSession.builder.appName("ChannelFlowAnalysis").getOrCreate()

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

def path_exists(path):
    return hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path))

# ---------------- TIMESTAMP NORMALIZATION ----------------
def normalize_bad_ampm(df, ts_col):
    df = df.withColumn(
        "_hour_24",
        regexp_extract(col(ts_col), r"\s(\d{1,2}):", 1).cast("int")
    )

    df = df.withColumn(
        "_norm_ts",
        when(
            (col(ts_col).rlike("AM|PM")) & (col("_hour_24") > 12),
            regexp_replace(
                col(ts_col),
                r"\s\d{1,2}:",
                concat_ws(
                    "",
                    lit(" "),
                    lpad((col("_hour_24") - 12).cast("string"), 2, "0"),
                    lit(":")
                )
            )
        ).otherwise(col(ts_col))
    )

    return df.drop("_hour_24")

def parse_timestamp(df, ts_col):
    df = normalize_bad_ampm(df, ts_col)

    df = df.withColumn(
        "event_ts",
        coalesce(
            to_timestamp(col("_norm_ts"), "dd-MM-yy hh:mm:ss a"),
            to_timestamp(col("_norm_ts"), "dd-MM-yyyy hh:mm:ss a"),
            to_timestamp(col("_norm_ts"), "dd-MM-yyyy HH:mm:ss"),
            to_timestamp(col("_norm_ts"), "dd-MM-yyyy HH:mm"),
            to_timestamp(col("_norm_ts"), "dd-MM-yy HH:mm:ss"),
            to_timestamp(col("_norm_ts"), "dd-MM-yy HH:mm")
        )
    )

    return df.drop("_norm_ts")

# ---------------- READERS ----------------
def read_stacy(path):
    df = spark.read.csv(path, header=True)

    if "HKT" in df.columns:
        ts_col = "HKT"
    else:
        ts_col = "date (UTC)"

    if "user_id" in df.columns:
        user_col = "user_id"
    else:
        user_col = "customer_id"

    df = parse_timestamp(df, ts_col)

    return df.select(
        col(user_col).alias("user_id"),
        col("event_ts"),
        lit("Stacy").alias("channel")
    )

def read_ivr(path):
    df = spark.read.csv(path, header=True)
    df = df.filter(col("ONE_FA").isin(post_login_values))
    df = parse_timestamp(df, "STARTTIME")

    return df.select(
        col("REL_ID").alias("user_id"),
        col("event_ts"),
        lit("IVR").alias("channel")
    )

def read_call(path):
    df = spark.read.csv(path, header=True)
    df = parse_timestamp(df, "Call Start Time")

    return df.select(
        col("Customer No (CTI)").alias("user_id"),
        col("event_ts"),
        lit("Call").alias("channel")
    )

def read_chat(path):
    df = spark.read.csv(path, header=True)
    df = df.filter(col("Pre/Post") == "Postlogin")

    df = df.withColumn(
        "_ts",
        concat_ws(" ", col("Date28"), col("StartTime"))
    )

    df = parse_timestamp(df, "_ts")

    return df.select(
        col("REL ID").alias("user_id"),
        col("event_ts"),
        lit("Chat").alias("channel")
    )

# ---------------- LOAD DATA ----------------
dfs = []

for cnt in stacy_cnt:
    for auth in stacy_auth:
        path = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_{auth}login.csv"
        if path_exists(path):
            dfs.append(read_stacy(path))

for i in range(1, 5):
    path = f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv"
    if path_exists(path):
        dfs.append(read_ivr(path))

call_path = f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"
if path_exists(call_path):
    dfs.append(read_call(call_path))

chat_path = f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv"
if path_exists(chat_path):
    dfs.append(read_chat(chat_path))

combined_df = dfs[0]
for d in dfs[1:]:
    combined_df = combined_df.unionByName(d)

combined_df = combined_df.dropna(subset=["user_id", "event_ts"]).cache()

# ---------------- SEQUENCING ----------------
w = Window.partitionBy("user_id").orderBy("event_ts")
df = combined_df.withColumn("rn", row_number().over(w))

# ---------------- AGGREGATION ----------------
results = []

for ch in channels:
    base = df.filter((col("rn") == 1) & (col("channel") == ch)) \
             .select("user_id").distinct()

    scoped = df.join(base, "user_id")

    agg = scoped.agg(
        count("*").alias("Total_case"),
        countDistinct("user_id").alias("uniq_cust")
    ).collect()[0]

    total_case = agg["Total_case"]
    uniq_cust = agg["uniq_cust"]
    rep_rate = (total_case - uniq_cust) * 100 / total_case if total_case else 0

    followups = scoped.groupBy("user_id").count()

    bucket = followups.withColumn(
        "bucket",
        when(col("count") == 1, "0")
        .when(col("count") == 2, "1")
        .when(col("count") == 3, "2")
        .otherwise("3+")
    ).groupBy("bucket").count()

    bucket_map = {r["bucket"]: r["count"] for r in bucket.collect()}

    def top_channels(n):
        rows = (
            scoped.filter(col("rn") == n)
            .groupBy("channel").count()
            .orderBy(col("count").desc())
            .limit(4)
            .collect()
        )
        chs = [r["channel"] for r in rows] + [""] * (4 - len(rows))
        cnts = [r["count"] for r in rows] + [0] * (4 - len(rows))
        return chs, cnts

    sec_ch, sec_ct = top_channels(2)
    thr_ch, thr_ct = top_channels(3)

    results.append([
        part, ch, total_case, uniq_cust, rep_rate,
        bucket_map.get("0", 0),
        bucket_map.get("1", 0),
        bucket_map.get("2", 0),
        bucket_map.get("3+", 0),
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
