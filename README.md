from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_timestamp, concat_ws, row_number,
    format_number, when
)
from pyspark.sql.window import Window
from functools import reduce

spark = SparkSession.builder.appName("CallCentreFullAnalytics").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "Asia/Hong_Kong")

# =========================
# MONTHS & CLIENT BASE
# =========================
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

post_login_values = ["OTP|S", "VB|S", "TPIN|S"]

hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

# =========================
# NORMALIZER WITH DEBUG
# =========================
def normalize(df, channel, user_col, ts_type):
    print(f"\n--- NORMALIZING {channel} ---")
    print("Input rows:", df.count())
    print("Columns:", df.columns)

    if ts_type == "single":
        ts_expr = to_timestamp(col("start_time"), "dd-MM-yyyy HH:mm")
    elif ts_type == "chat":
        ts_expr = to_timestamp(
            concat_ws(" ", col("date"), col("time")),
            "dd-MM-yyyy HH:mm:ss"
        )
    else:
        raise Exception("Invalid ts_type")

    parsed = df.select(
        col(user_col).cast("string").alias("customer_id"),
        ts_expr.alias("event_ts")
    )

    parsed.show(5, False)

    valid = parsed.filter(
        col("customer_id").isNotNull() &
        col("event_ts").isNotNull()
    )

    print("Valid rows:", valid.count())

    return valid.withColumn("channel", lit(channel))

# =========================
# RESULT HOLDERS
# =========================
overview = []
overview_post = []
unique_data = []
ratio_data = []
all_months_flow = []

# =========================
# MAIN LOOP
# =========================
for part, mnth in months.items():
    print(f"\n================ {part} ================")

    stacy_dfs = []
    ivr_dfs = []

    # ===== STACY =====
    for lang in ["en", "zh"]:
        for auth in ["pre", "post"]:
            path = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{lang}_{auth}login.csv"
            if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path)):
                df = spark.read.csv(path, header=True)
                user_col = "user_id" if "user_id" in df.columns else "customer_id"
                stacy_dfs.append((auth, df))

    # ===== IVR =====
    for i in range(1, 5):
        path = f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv"
        if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path)):
            ivr_dfs.append(spark.read.csv(path, header=True))

    # ===== CALL =====
    call_path = f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"
    df_call = spark.read.csv(call_path, header=True) if hadoop_fs.exists(
        spark._jvm.org.apache.hadoop.fs.Path(call_path)
    ) else None

    # ===== CHAT =====
    chat_path = f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv"
    df_chat = spark.read.csv(chat_path, header=True) if hadoop_fs.exists(
        spark._jvm.org.apache.hadoop.fs.Path(chat_path)
    ) else None

    # =========================
    # COUNTS
    # =========================
    count_stacy = sum(df.count() for _, df in stacy_dfs)
    count_ivr = sum(df.count() for df in ivr_dfs)
    count_call = df_call.count() if df_call else 0
    count_chat = df_chat.count() if df_chat else 0

    overview += [
        (part, "Stacy", count_stacy),
        (part, "IVR", count_ivr),
        (part, "Call", count_call),
        (part, "Live Chat", count_chat)
    ]

    # =========================
    # POST LOGIN COUNTS
    # =========================
    count_stacy_post = sum(df.count() for a, df in stacy_dfs if a == "post")
    count_ivr_post = sum(df.filter(col("ONE_FA").isin(post_login_values)).count() for df in ivr_dfs)
    count_call_post = df_call.filter(col("Verification Status") == "Pass").count() if df_call else 0
    count_chat_post = df_chat.filter(col("Pre/Post") == "Postlogin").count() if df_chat else 0

    overview_post += [
        (part, "Stacy", count_stacy_post),
        (part, "IVR", count_ivr_post),
        (part, "Call", count_call_post),
        (part, "Live Chat", count_chat_post)
    ]

    # =========================
    # UNIQUE CUSTOMERS
    # =========================
    uniq_stacy = reduce(
        lambda a, b: a.union(b),
        [df.select(user_col).cast("string") for a, df in stacy_dfs if a == "post"]
    ).distinct().count() if count_stacy_post > 0 else 0

    uniq_ivr = reduce(
        lambda a, b: a.union(b),
        [df.select("REL_ID").cast("string") for df in ivr_dfs]
    ).distinct().count() if count_ivr > 0 else 0

    uniq_call = df_call.select(col("Customer No (CTI)").cast("string")).distinct().count() if df_call else 0
    uniq_chat = df_chat.select(col("REL ID").cast("string")).distinct().count() if df_chat else 0

    unique_data += [
        (part, "Stacy", uniq_stacy),
        (part, "IVR", uniq_ivr),
        (part, "Call", uniq_call),
        (part, "Live Chat", uniq_chat)
    ]

    total_clients = total_HK_clients[part]

    ratio_data += [
        (part, "Stacy", uniq_stacy / total_clients * 100),
        (part, "IVR", uniq_ivr / total_clients * 100),
        (part, "Call", uniq_call / total_clients * 100),
        (part, "Live Chat", uniq_chat / total_clients * 100)
    ]

    # =========================
    # FLOW DATA (POST LOGIN ONLY)
    # =========================
    flow_parts = []

    for auth, df in stacy_dfs:
        if auth == "post":
            user_col = "user_id" if "user_id" in df.columns else "customer_id"
            flow_parts.append(normalize(df, "Stacy", user_col, "single"))

    for df in ivr_dfs:
        flow_parts.append(
            normalize(
                df.filter(col("ONE_FA").isin(post_login_values)),
                "IVR", "REL_ID", "single"
            )
        )

    if df_call:
        flow_parts.append(
            normalize(
                df_call.filter(col("Verification Status") == "Pass"),
                "Call", "Customer No (CTI)", "single"
            )
        )

    if df_chat:
        df_chat = df_chat.filter(col("Pre/Post") == "Postlogin")
        flow_parts.append(
            normalize(df_chat, "Live Chat", "REL ID", "chat")
        )

    if flow_parts:
        all_months_flow.append(
            reduce(lambda a, b: a.unionByName(b), flow_parts)
            .withColumn("month", lit(part))
        )

# =========================
# FINAL DATAFRAMES
# =========================
overview_df = spark.createDataFrame(overview, ["month", "channel", "volume"]) \
    .withColumn("volume_fmt", format_number("volume", 0))

overview_post_df = spark.createDataFrame(overview_post, ["month", "channel", "volume"]) \
    .withColumn("volume_fmt", format_number("volume", 0))

unique_df = spark.createDataFrame(unique_data, ["month", "channel", "volume"]) \
    .withColumn("volume_fmt", format_number("volume", 0))

ratio_df = spark.createDataFrame(ratio_data, ["month", "channel", "ratio"]) \
    .withColumn("ratio_fmt", format_number("ratio", 2))

combined_flow = reduce(lambda a, b: a.unionByName(b), all_months_flow)

w = Window.partitionBy("month", "customer_id").orderBy("event_ts")
flow_df = combined_flow.withColumn("contact_order", row_number().over(w))

first_contact = flow_df.filter(col("contact_order") == 1) \
    .groupBy("month", "channel").count()

stacy_start = flow_df.filter(
    (col("contact_order") == 1) & (col("channel") == "Stacy")
)

second_channel = flow_df.filter(col("contact_order") == 2) \
    .groupBy("month", "channel").count()

followups = flow_df.join(
    stacy_start.select("month", "customer_id"),
    ["month", "customer_id"]
).filter(col("contact_order") > 1)

followup_dist = followups.groupBy("month", "customer_id").count()

# =========================
# SHOW RESULTS
# =========================
overview_df.show()
overview_post_df.show()
unique_df.show()
ratio_df.show()
first_contact.show()
second_channel.show()
followup_dist.show()
