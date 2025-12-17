from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_timestamp, concat_ws, row_number,
    when, format_number
)
from pyspark.sql.window import Window
from functools import reduce

spark = SparkSession.builder.appName("CallCentreFullAnalytics").getOrCreate()

months = {
    "202501": "jan", "202502": "feb", "202503": "mar",
    "202504": "apr", "202505": "may", "202506": "jun",
    "202507": "jul", "202508": "aug", "202509": "sep",
    "202510": "oct", "202511": "nov"
}

total_HK_clients = {
    "202501": 1748757, "202502": 1749000, "202503": 1750000,
    "202504": 1751000, "202505": 1752000, "202506": 1753000,
    "202507": 1754000, "202508": 1755000, "202509": 1756000,
    "202510": 1757000, "202511": 1758000
}

stacy_cnt = ["en", "zh"]
stacy_auth = ["pre", "post"]
post_login_values = ["OTP|S", "VB|S", "TPIN|S"]

hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

def normalize(df, user_col, ts_col, channel):
    return (
        df.select(
            col(user_col).alias("customer_id"),
            to_timestamp(col(ts_col)).alias("event_ts")
        )
        .withColumn("channel", lit(channel))
        .filter(col("customer_id").isNotNull())
        .filter(col("event_ts").isNotNull())
    )

overview_data = []
overview_post_data = []
unique_channel_data = []
contact_ratio_data = []
all_months_df = []

for part, mnth in months.items():
    stacy_dfs = {}
    for cnt in stacy_cnt:
        for auth in stacy_auth:
            path = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_{auth}login.csv"
            if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path)):
                stacy_dfs[f"{cnt}_{auth}"] = spark.read.csv(path, header=True, inferSchema=True)

    ivr_dfs = []
    for i in range(1, 5):
        path = f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv"
        if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path)):
            ivr_dfs.append(spark.read.csv(path, header=True, inferSchema=True))

    call_path = f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"
    chat_path = f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv"

    df_call = spark.read.csv(call_path, header=True, inferSchema=True) if hadoop_fs.exists(
        spark._jvm.org.apache.hadoop.fs.Path(call_path)
    ) else None

    df_chat = spark.read.csv(chat_path, header=True, inferSchema=True) if hadoop_fs.exists(
        spark._jvm.org.apache.hadoop.fs.Path(chat_path)
    ) else None

    count_stacy = sum(df.count() for df in stacy_dfs.values())
    count_ivr = sum(df.count() for df in ivr_dfs)
    count_call = df_call.count() if df_call else 0
    count_chat = df_chat.count() if df_chat else 0

    overview_data += [
        (part, "Stacy", count_stacy),
        (part, "IVR", count_ivr),
        (part, "Call", count_call),
        (part, "Live Chat", count_chat)
    ]

    count_stacy_post = sum(df.count() for k, df in stacy_dfs.items() if "post" in k)
    count_ivr_post = sum(df.filter(col("ONE_FA").isin(post_login_values)).count() for df in ivr_dfs)
    count_call_post = df_call.filter(col("Verification_Status") == "Pass").count() if df_call else 0
    count_chat_post = df_chat.filter(col("Pre/Post") == "Postlogin").count() if df_chat else 0

    overview_post_data += [
        (part, "Stacy", count_stacy_post),
        (part, "IVR", count_ivr_post),
        (part, "Call", count_call_post),
        (part, "Live Chat", count_chat_post)
    ]

    uniq_stacy = (
        reduce(lambda a, b: a.union(b),
               [df.select(col("user_id").alias("cid")) for k, df in stacy_dfs.items() if "post" in k])
        .distinct().count() if count_stacy_post > 0 else 0
    )

    uniq_ivr = (
        reduce(lambda a, b: a.union(b),
               [df.select(col("REL_ID").alias("cid")) for df in ivr_dfs])
        .distinct().count() if count_ivr > 0 else 0
    )

    uniq_call = df_call.select(col("Customer_No_(CTI)").alias("cid")).distinct().count() if df_call else 0
    uniq_chat = df_chat.select(col("REL ID").alias("cid")).distinct().count() if df_chat else 0

    unique_channel_data += [
        (part, "Stacy", uniq_stacy),
        (part, "IVR", uniq_ivr),
        (part, "Call", uniq_call),
        (part, "Live Chat", uniq_chat)
    ]

    total_clients = total_HK_clients[part]

    contact_ratio_data += [
        (part, "Stacy", uniq_stacy / total_clients * 100),
        (part, "IVR", uniq_ivr / total_clients * 100),
        (part, "Call", uniq_call / total_clients * 100),
        (part, "Live Chat", uniq_chat / total_clients * 100)
    ]

    stacy_post = [normalize(df, "user_id", "start_time", "Stacy")
                  for k, df in stacy_dfs.items() if "post" in k]

    ivr_post = [
        normalize(df.filter(col("ONE_FA").isin(post_login_values)), "REL_ID", "start_time", "IVR")
        for df in ivr_dfs
    ]

    call_post = normalize(
        df_call.filter(col("Verification_Status") == "Pass"),
        "Customer_No_(CTI)", "start_time", "Call"
    ) if df_call else None

    chat_post = normalize(
        df_chat.filter(col("Pre/Post") == "Postlogin")
               .withColumn("event_ts", concat_ws(" ", col("date"), col("time"))),
        "REL ID", "event_ts", "Live Chat"
    ) if df_chat else None

    union_list = stacy_post + ivr_post
    if call_post:
        union_list.append(call_post)
    if chat_post:
        union_list.append(chat_post)

    if union_list:
        all_months_df.append(
            reduce(lambda a, b: a.unionByName(b), union_list)
            .withColumn("month", lit(part))
        )

overview_df = spark.createDataFrame(
    overview_data, ["month", "channel", "volume"]
).withColumn("volume_fmt", format_number("volume", 0))

overview_post_df = spark.createDataFrame(
    overview_post_data, ["month", "channel", "volume"]
).withColumn("volume_fmt", format_number("volume", 0))

unique_df = spark.createDataFrame(
    unique_channel_data, ["month", "channel", "volume"]
).withColumn("volume_fmt", format_number("volume", 0))

ratio_df = spark.createDataFrame(
    contact_ratio_data, ["month", "channel", "ratio"]
).withColumn("ratio_fmt", format_number("ratio", 2))

combined_df = reduce(lambda a, b: a.unionByName(b), all_months_df)

w = Window.partitionBy("month", "customer_id").orderBy("event_ts")

flow_df = combined_df.withColumn("contact_order", row_number().over(w))

first_contact = flow_df.filter(col("contact_order") == 1) \
    .groupBy("month", "channel").count()

stacy_start = flow_df.filter(
    (col("contact_order") == 1) & (col("channel") == "Stacy")
)

stacy_cases = stacy_start.groupBy("month").count()

stacy_customers = stacy_start.select("month", "customer_id").distinct() \
    .groupBy("month").count()

followups = flow_df.join(
    stacy_start.select("month", "customer_id"),
    ["month", "customer_id"]
).filter(col("contact_order") > 1) \
 .groupBy("month", "customer_id").count()

followup_dist = followups.withColumn(
    "bucket",
    when(col("count") == 1, "1")
    .when(col("count") == 2, "2")
    .when(col("count") >= 3, "3+")
).groupBy("month", "bucket").count()

second_channel = flow_df.join(
    stacy_start.select("month", "customer_id"),
    ["month", "customer_id"]
).filter(col("contact_order") == 2) \
 .groupBy("month", "channel").count()

overview_df.show()
overview_post_df.show()
unique_df.show()
ratio_df.show()
first_contact.show()
stacy_cases.show()
stacy_customers.show()
followup_dist.show()
second_channel.show()
