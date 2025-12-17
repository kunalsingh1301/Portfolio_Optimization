from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_timestamp, concat_ws, row_number,
    format_number
)
from pyspark.sql.window import Window
from functools import reduce

spark = SparkSession.builder.appName("CallCentreFinal").getOrCreate()
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
# UNIVERSAL NORMALIZER
# =========================
def normalize(df, channel, user_col):
    print(f"\n--- NORMALIZING {channel} ---")
    print("Columns:", df.columns)
    print("Rows before:", df.count())

    if "HKT" in df.columns:
        ts_expr = to_timestamp(col("HKT"), "dd-MM-yyyy HH:mm")
    elif "start_time" in df.columns:
        ts_expr = to_timestamp(col("start_time"), "dd-MM-yyyy HH:mm")
    elif set(["date", "time"]).issubset(df.columns):
        ts_expr = to_timestamp(
            concat_ws(" ", col("date"), col("time")),
            "dd-MM-yyyy HH:mm:ss"
        )
    else:
        print("❌ No timestamp column found")
        return None

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
# HOLDERS
# =========================
overview = []
overview_post = []
unique_data = []
ratio_data = []
all_flow = []

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
                stacy_dfs.append((auth, df, user_col))

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
    # FLOW (POST LOGIN ONLY)
    # =========================
    flow_parts = []

    for auth, df, user_col in stacy_dfs:
        if auth == "post":
            norm = normalize(df, "Stacy", user_col)
            if norm:
                flow_parts.append(norm)

    for df in ivr_dfs:
        norm = normalize(
            df.filter(col("ONE_FA").isin(post_login_values)),
            "IVR", "REL_ID"
        )
        if norm:
            flow_parts.append(norm)

    if df_call:
        norm = normalize(
            df_call.filter(col("Verification Status") == "Pass"),
            "Call", "Customer No (CTI)"
        )
        if norm:
            flow_parts.append(norm)

    if df_chat:
        norm = normalize(
            df_chat.filter(col("Pre/Post") == "Postlogin"),
            "Live Chat", "REL ID"
        )
        if norm:
            flow_parts.append(norm)

    if flow_parts:
        all_flow.append(
            reduce(lambda a, b: a.unionByName(b), flow_parts)
            .withColumn("month", lit(part))
        )

# =========================
# FLOW ANALYSIS
# =========================
combined_flow = reduce(lambda a, b: a.unionByName(b), all_flow)
print("\nFINAL FLOW COUNT:", combined_flow.count())

w = Window.partitionBy("month", "customer_id").orderBy("event_ts")
flow_df = combined_flow.withColumn("contact_order", row_number().over(w))

first_contact = flow_df.filter(col("contact_order") == 1) \
    .groupBy("month", "channel").count()

second_channel = flow_df.filter(col("contact_order") == 2) \
    .groupBy("month", "channel").count()

followups = flow_df.filter(col("contact_order") > 1) \
    .groupBy("month", "customer_id").count()

# =========================
# SHOW
# =========================
combined_flow.show(10, False)
first_contact.show()
second_channel.show()
followups.show()
