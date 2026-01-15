from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from functools import reduce

# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("PostLoginFlow_NoTimestamp") \
    .enableHiveSupport() \
    .getOrCreate()

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
mnth = "jan"
part = "202501"

stacy_cnt = ["en", "zh"]
stacy_auth = ["post"]
post_login_values = ["OTP|S", "VB|S", "TPIN|S"]

# --------------------------------------------------
# HDFS HELPERS
# --------------------------------------------------
hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

def path_exists(p):
    return hadoop_fs.exists(
        spark._jvm.org.apache.hadoop.fs.Path(p)
    )

def safe_read(func, path):
    try:
        if path_exists(path):
            df = func(path)
            if df is not None and not df.rdd.isEmpty():
                return df
    except Exception as e:
        print(f"Error reading {path}: {e}")
    return None

# --------------------------------------------------
# READERS (POSTLOGIN ONLY)
# --------------------------------------------------
def read_stacy(path):
    df = spark.read.option("header", True).csv(path)
    user_col = "user_id" if "user_id" in df.columns else "customer_id"
    return (
        df.filter(col(user_col).isNotNull())
          .select(col(user_col).alias("user_id"))
          .withColumn("channel", lit("Stacy"))
    )

def read_chat(path):
    df = spark.read.option("header", True).csv(path)
    return (
        df.filter(col("Pre/Post") == "Postlogin")
          .filter(col("REL ID").isNotNull())
          .select(col("REL ID").alias("user_id"))
          .withColumn("channel", lit("Chat"))
    )

def read_call(path):
    df = spark.read.option("header", True).csv(path)
    return (
        df.filter(col("Verification Status") == "Pass")
          .filter(col("Customer No (CTI)").isNotNull())
          .select(col("Customer No (CTI)").alias("user_id"))
          .withColumn("channel", lit("Call"))
    )

def read_ivr(path):
    df = spark.read.option("header", True).csv(path)
    return (
        df.filter(col("ONE_FA").isin(post_login_values))
          .filter(col("REL_ID").isNotNull())
          .select(col("REL_ID").alias("user_id"))
          .withColumn("channel", lit("IVR"))
    )

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------
dfs = []

for cnt in stacy_cnt:
    for auth in stacy_auth:
        p = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_{auth}login.csv"
        df = safe_read(read_stacy, p)
        if df is not None:
            dfs.append(df)

df = safe_read(read_chat, f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv")
if df is not None:
    dfs.append(df)

df = safe_read(read_call, f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv")
if df is not None:
    dfs.append(df)

for i in range(1, 5):
    p = f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv"
    df = safe_read(read_ivr, p)
    if df is not None:
        dfs.append(df)

if not dfs:
    raise ValueError("No postlogin data found")

combined_df = reduce(lambda a, b: a.unionByName(b), dfs).cache()

# --------------------------------------------------
# GLOBAL TOTALS
# --------------------------------------------------
total_postlogin_cases = combined_df.count()
total_distinct_users = combined_df.select("user_id").distinct().count()

# --------------------------------------------------
# CONTACT COUNT PER USER (MONTH)
# --------------------------------------------------
user_contact_cnt = (
    combined_df.groupBy("user_id")
    .agg(count("*").alias("user_total_contacts"))
)

df = combined_df.join(user_contact_cnt, "user_id")

# --------------------------------------------------
# FOLLOW-UP BUCKET (CASE LEVEL)
# --------------------------------------------------
df = df.withColumn(
    "follow_bucket",
    when(col("user_total_contacts") == 1, "0")
    .when(col("user_total_contacts") == 2, "1")
    .when(col("user_total_contacts") == 3, "2")
    .otherwise("3+")
)

# --------------------------------------------------
# TOTAL CASES PER CHANNEL
# --------------------------------------------------
total_case_df = (
    df.groupBy("channel")
      .agg(count("*").alias("total_case"))
)

# --------------------------------------------------
# FOLLOW-UP COUNTS (CASE LEVEL)
# --------------------------------------------------
bucket_df = (
    df.groupBy("channel", "follow_bucket")
      .count()
      .groupBy("channel")
      .pivot("follow_bucket", ["0", "1", "2", "3+"])
      .sum("count")
      .fillna(0)
      .withColumnRenamed("0", "follow_up_0")
      .withColumnRenamed("1", "follow_up_1")
      .withColumnRenamed("2", "follow_up_2")
      .withColumnRenamed("3+", "follow_up_3+")
)

# --------------------------------------------------
# SECOND / THIRD CHANNEL (CASE LEVEL)
# --------------------------------------------------
user_channels = (
    combined_df.groupBy("user_id")
    .agg(collect_set("channel").alias("channels_used"))
)

df = df.join(user_channels, "user_id")

df = df.withColumn(
    "other_channels",
    expr("filter(channels_used, x -> x != channel)")
)

second_df = (
    df.filter(size(col("other_channels")) >= 1)
      .withColumn("sec_channel", col("other_channels")[0])
      .groupBy("channel", "sec_channel")
      .count()
)

third_df = (
    df.filter(size(col("other_channels")) >= 2)
      .withColumn("third_channel", col("other_channels")[1])
      .groupBy("channel", "third_channel")
      .count()
)

def pivot_rank(df, col_name, prefix):
    w = Window.partitionBy("channel").orderBy(col("count").desc())
    ranked = df.withColumn("r", row_number().over(w)).filter(col("r") <= 4)

    exprs = []
    for i in range(1, 5):
        exprs += [
            max(when(col("r") == i, col(col_name))).alias(f"{prefix}_chnl_{i}"),
            max(when(col("r") == i, col("count"))).alias(f"{prefix}_chnl_count_{i}")
        ]

    return ranked.groupBy("channel").agg(*exprs)

sec_pivot = pivot_rank(second_df, "sec_channel", "sec")
third_pivot = pivot_rank(third_df, "third_channel", "third")

# --------------------------------------------------
# FINAL ASSEMBLY
# --------------------------------------------------
final_df = (
    total_case_df
    .join(bucket_df, "channel")
    .join(sec_pivot, "channel", "left")
    .join(third_pivot, "channel", "left")
    .withColumn("Date", lit(part))
    .withColumn("total_postlogin_cases", lit(total_postlogin_cases))
    .withColumn("total_distinct_users", lit(total_distinct_users))
    .withColumn(
        "rep_rate",
        (col("follow_up_1") + col("follow_up_2") + col("follow_up_3+")) / col("total_case")
    )
    .withColumnRenamed("channel", "Channel")
)

# --------------------------------------------------
# FINAL COLUMN ORDER
# --------------------------------------------------
columns = [
    "Date", "Channel",
    "total_postlogin_cases", "total_distinct_users",
    "total_case", "rep_rate",
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

# --------------------------------------------------
# SHOW RESULT
# --------------------------------------------------
final_df.show(truncate=False)
