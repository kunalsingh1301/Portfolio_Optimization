from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, substring, concat_ws, to_timestamp,
    row_number, count
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("FirstContactAnalysis").getOrCreate()

mnth = "jan"
stacy_cnt = ["en", "zh"]
stacy_auth = ["post"]
post_login_values = ["OTP|S", "VB|S", "TPIN|S"]

hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

# ---------------- READERS ---------------- #

def read_stacy(path):
    df = spark.read.csv(path, header=True)
    if "HKT" in df.columns:
        df = df.withColumn("date", substring("HKT", 1, 10)) \
               .withColumn("time", substring("HKT", 12, 5))
    elif "date (UTC)" in df.columns:
        df = df.withColumn("date", substring("date (UTC)", 1, 10)) \
               .withColumn("time", substring("date (UTC)", 12, 5))

    user_col = "user_id" if "user_id" in df.columns else "customer_id"
    return df.select(col(user_col).alias("user_id"), "date", "time") \
             .withColumn("channel", lit("Stacy"))


def read_ivr(path):
    df = spark.read.csv(path, header=True)
    df = df.filter(col("ONE_FA").isin(post_login_values))
    df = df.withColumn("date", substring("STARTTIME", 1, 10)) \
           .withColumn("time", substring("STARTTIME", 12, 5))
    return df.select(col("REL_ID").alias("user_id"), "date", "time") \
             .withColumn("channel", lit("IVR"))


def read_call(path):
    df = spark.read.csv(path, header=True)
    df = df.withColumn("date", substring("Call Start Time", 1, 10)) \
           .withColumn("time", substring("Call Start Time", 12, 5))
    return df.select(col("Customer No (CTI)").alias("user_id"), "date", "time") \
             .withColumn("channel", lit("Call"))


def read_chat(path):
    df = spark.read.csv(path, header=True)
    df = df.filter(col("Pre/Post") == "Postlogin")
    df = df.withColumn("time", substring("StartTime", 1, 5))
    return df.select(col("REL ID").alias("user_id"),
                     col("Date28").alias("date"),
                     "time") \
             .withColumn("channel", lit("Chat"))

# ---------------- LOAD DATA ---------------- #

dfs = []

for cnt in stacy_cnt:
    path = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_postlogin.csv"
    if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path)):
        dfs.append(read_stacy(path))

for i in range(1, 5):
    path = f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv"
    if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path)):
        dfs.append(read_ivr(path))

call_path = f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"
if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(call_path)):
    dfs.append(read_call(call_path))

chat_path = f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv"
if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(chat_path)):
    dfs.append(read_chat(chat_path))

# ---------------- COMBINE ---------------- #

df = dfs[0]
for d in dfs[1:]:
    df = df.unionByName(d)

df = df.dropna(subset=["user_id", "date", "time"])

# ---------------- TIMESTAMP ---------------- #

df = df.withColumn(
    "event_ts",
    to_timestamp(concat_ws(" ", col("date"), col("time")), "dd-MM-yyyy HH:mm")
)

# ---------------- ORDER PER USER ---------------- #

w = Window.partitionBy("user_id").orderBy("event_ts")

df_ranked = df.withColumn("rn", row_number().over(w))

# ---------------- FIRST CONTACT ---------------- #

df_first = df_ranked.filter(col("rn") == 1)

print("FIRST CONTACT CHANNEL COUNTS")
df_first.groupBy("channel").count().show()

# ---------------- SECOND CONTACT ---------------- #

df_second = df_ranked.filter(col("rn") == 2)

print("SECOND CONTACT CHANNEL COUNTS")
df_second.groupBy("channel").count().show()

# ---------------- FOLLOW-UP DISTRIBUTION ---------------- #

print("FOLLOW-UP COUNT DISTRIBUTION")
df_ranked.groupBy("user_id").count() \
    .groupBy("count").agg(count("*").alias("users")) \
    .orderBy("count").show()

# ---------------- DEBUG ---------------- #

print("DEBUG SAMPLE ORDERING")
df_ranked.orderBy("user_id", "event_ts").show(20, False)
