from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_timestamp, concat_ws, row_number,
    when, coalesce, format_number
)
from pyspark.sql.window import Window
from functools import reduce

spark = SparkSession.builder.appName("CallCentreFullAnalytics").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "Asia/Hong_Kong")

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

def resolve_user_col(df):
    for c in ["user_id", "customer_id", "REL_ID", "Customer No (CTI)"]:
        if c in df.columns:
            return c
    return None

def resolve_timestamp_expr(df):
    exprs = []

    if "start_time" in df.columns:
        exprs += [
            to_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("start_time"), "yyyy-MM-dd'T'HH:mm:ss"),
            to_timestamp(col("start_time"), "yyyy/MM/dd HH:mm:ss")
        ]

    if "HKT" in df.columns:
        exprs += [
            to_timestamp(col("HKT"), "yyyyMMdd HH:mm:ss"),
            to_timestamp(col("HKT"), "yyyyMMdd HHmmss"),
            to_timestamp(col("HKT"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("HKT"), "dd/MM/yyyy HH:mm:ss")
        ]

    if "Date7" in df.columns and "StartTime" in df.columns:
        exprs += [
            to_timestamp(concat_ws(" ", col("Date7"), col("StartTime")), "yyyyMMdd HHmmss"),
            to_timestamp(concat_ws(" ", col("Date7"), col("StartTime")), "yyyyMMdd HH:mm:ss")
        ]

    if "date" in df.columns and "time" in df.columns:
        exprs += [
            to_timestamp(concat_ws(" ", col("date"), col("time")), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(concat_ws(" ", col("date"), col("time")), "dd/MM/yyyy HH:mm:ss")
        ]

    return coalesce(*exprs) if exprs else None

def normalize(df, channel):
    user_col = resolve_user_col(df)
    ts_expr = resolve_timestamp_expr(df)

    if user_col is None or ts_expr is None:
        return spark.createDataFrame([], "customer_id STRING, event_ts TIMESTAMP, channel STRING")

    return (
        df.select(
            col(user_col).cast("string").alias("customer_id"),
            ts_expr.alias("event_ts")
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
    stacy_dfs = []
    for cnt in stacy_cnt:
        for auth in stacy_auth:
            p = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_{auth}login.csv"
            if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p)):
                stacy_dfs.append((auth, spark.read.csv(p, header=True, inferSchema=True)))

    ivr_dfs = []
    for i in range(1, 5):
        p = f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv"
        if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p)):
            ivr_dfs.append(spark.read.csv(p, header=True, inferSchema=True))

    call_path = f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"
    chat_path = f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv"

    df_call = spark.read.csv(call_path, header=True, inferSchema=True) if hadoop_fs.exists(
        spark._jvm.org.apache.hadoop.fs.Path(call_path)) else None

    df_chat = spark.read.csv(chat_path, header=True, inferSchema=True) if hadoop_fs.exists(
        spark._jvm.org.apache.hadoop.fs.Path(chat_path)) else None

    overview_data += [
        (part, "Stacy", sum(df.count() for _, df in stacy_dfs)),
        (part, "IVR", sum(df.count() for df in ivr_dfs)),
        (part, "Call", df_call.count() if df_call else 0),
        (part, "Live Chat", df_chat.count() if df_chat else 0)
    ]

    overview_post_data += [
        (part, "Stacy", sum(df.count() for a, df in stacy_dfs if a == "post")),
        (part, "IVR", sum(df.filter(col("ONE_FA").isin(post_login_values)).count() for df in ivr_dfs)),
        (part, "Call", df_call.filter(col("Verification Status") == "Pass").count() if df_call else 0),
        (part, "Live Chat", df_chat.filter(col("Pre/Post") == "Postlogin").count() if df_chat else 0)
    ]

    uniq_stacy_ids = [
        df.select(resolve_user_col(df)).alias("cid")
        for a, df in stacy_dfs if a == "post" and resolve_user_col(df) is not None
    ]

    uniq_stacy = (
        reduce(lambda a, b: a.unionByName(b), uniq_stacy_ids).distinct().count()
        if uniq_stacy_ids else 0
    )

    uniq_ivr = (
        reduce(lambda a, b: a.unionByName(b),
               [df.select("REL_ID").alias("cid") for df in ivr_dfs])
        .distinct().count() if ivr_dfs else 0
    )

    uniq_call = df_call.select("Customer No (CTI)").distinct().count() if df_call else 0
    uniq_chat = df_chat.select("REL ID").distinct().count() if df_chat else 0

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

    norm_dfs = []
    norm_dfs += [normalize(df, "Stacy") for a, df in stacy_dfs if a == "post"]
    norm_dfs += [normalize(df.filter(col("ONE_FA").isin(post_login_values)), "IVR") for df in ivr_dfs]

    if df_call:
        norm_dfs.append(normalize(df_call.filter(col("Verification Status") == "Pass"), "Call"))

    if df_chat:
        norm_dfs.append(normalize(df_chat.filter(col("Pre/Post") == "Postlogin"), "Live Chat"))

    if norm_dfs:
        all_months_df.append(
            reduce(lambda a, b: a.unionByName(b), norm_dfs).withColumn("month", lit(part))
        )

combined_df = reduce(lambda a, b: a.unionByName(b), all_months_df)

w = Window.partitionBy("month", "customer_id").orderBy("event_ts")
flow_df = combined_df.withColumn("contact_order", row_number().over(w))

first_contact = flow_df.filter(col("contact_order") == 1).groupBy("month", "channel").count()

stacy_start = flow_df.filter(
    (col("contact_order") == 1) & (col("channel") == "Stacy")
)

followups = flow_df.join(
    stacy_start.select("month", "customer_id"),
    ["month", "customer_id"]
).filter(col("contact_order") > 1)

followup_dist = followups.groupBy("month", "customer_id").count() \
    .withColumn(
        "bucket",
        when(col("count") == 1, "1")
        .when(col("count") == 2, "2")
        .otherwise("3+")
    ).groupBy("month", "bucket").count()

second_channel = flow_df.join(
    stacy_start.select("month", "customer_id"),
    ["month", "customer_id"]
).filter(col("contact_order") == 2) \
 .groupBy("month", "channel").count()

first_contact.show()
followup_dist.show()
second_channel.show()
