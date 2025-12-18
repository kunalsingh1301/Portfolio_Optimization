from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number, count, when, concat_ws, to_timestamp
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("JanChannelFlowAnalysis").getOrCreate()

mnth = "jan"
part = "202501"

stacy_cnt = ["en", "zh"]
stacy_auth = ["post"]
post_login_values = ["OTP|S", "VB|S", "TPIN|S"]

hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

def read_stacy(path):
    df = spark.read.csv(path, header=True)
    if "HKT" in df.columns:
        df = df.withColumn("event_ts", to_timestamp(col("HKT"), "dd-MM-yyyy HH:mm"))
    elif "date (UTC)" in df.columns:
        df = df.withColumn("event_ts", to_timestamp(col("date (UTC)"), "dd-MM-yyyy HH:mm"))
    if "user_id" in df.columns:
        df = df.select(col("user_id"), col("event_ts"))
    else:
        df = df.select(col("customer_id").alias("user_id"), col("event_ts"))
    return df.withColumn("channel", lit("Stacy"))

def read_ivr(path):
    df = spark.read.csv(path, header=True)
    df = df.filter(col("ONE_FA").isin(post_login_values))
    df = df.withColumn("event_ts", to_timestamp(col("STARTTIME"), "dd-MM-yyyy HH:mm"))
    return df.select(col("REL_ID").alias("user_id"), col("event_ts")) \
             .withColumn("channel", lit("IVR"))

def read_call(path):
    df = spark.read.csv(path, header=True)
    df = df.withColumn("event_ts", to_timestamp(col("Call Start Time"), "dd-MM-yyyy HH:mm"))
    return df.select(col("Customer No (CTI)").alias("user_id"), col("event_ts")) \
             .withColumn("channel", lit("Call"))

def read_chat(path):
    df = spark.read.csv(path, header=True)
    df = df.filter(col("Pre/Post") == "Postlogin")
    df = df.withColumn(
        "event_ts",
        to_timestamp(concat_ws(" ", col("Date28"), col("StartTime")), "dd-MM-yyyy HH:mm:ss")
    )
    return df.select(col("REL ID").alias("user_id"), col("event_ts")) \
             .withColumn("channel", lit("Chat"))

dfs = []

for cnt in stacy_cnt:
    for auth in stacy_auth:
        path = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_{auth}login.csv"
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

combined_df = dfs[0]
for d in dfs[1:]:
    combined_df = combined_df.union(d)

combined_df = combined_df.dropna(subset=["user_id", "event_ts"])

w = Window.partitionBy("user_id").orderBy("event_ts")
df_ranked = combined_df.withColumn("rn", row_number().over(w))

channels = ["Stacy", "IVR", "Call", "Chat"]

for ch in channels:
    starters = df_ranked.filter((col("rn") == 1) & (col("channel") == ch)) \
                         .select("user_id")

    if starters.count() == 0:
        continue

    scoped = df_ranked.join(starters, ["user_id"])

    total_cases = scoped.count()
    unique_customers = scoped.select("user_id").distinct().count()
    repeated_rate = (total_cases - unique_customers) / total_cases * 100

    print(f"\nCHANNEL: {ch}")
    print(f"Total Cases: {total_cases}")
    print(f"Unique Customers: {unique_customers}")
    print(f"Repeated Rate: {repeated_rate:.2f}%")

    followups = scoped.groupBy("user_id").count()

    followup_dist = followups.withColumn(
        "bucket",
        when(col("count") == 1, "0")
        .when(col("count") == 2, "1")
        .when(col("count") == 3, "2")
        .otherwise("3+")
    ).groupBy("bucket").count().orderBy("bucket")

    followup_dist.show()

    second_contact = scoped.filter(col("rn") == 2) \
        .groupBy("channel").count().orderBy("count", ascending=False)

    second_contact.show()
