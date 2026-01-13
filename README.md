from pyspark.sql import SparkSession, functions as F

# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------
spark = SparkSession.builder.appName("CallCentreAnalytics_Clean").getOrCreate()

hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
months = {
    '202501': 'jan', '202502': 'feb', '202503': 'mar',
    '202504': 'apr', '202505': 'may', '202506': 'jun',
    '202507': 'jul', '202508': 'aug', '202509': 'sep',
    '202510': 'oct', '202511': 'nov'
}

total_HK_clients = {
    '202501': 1748757, '202502': 1750599, '202503': 1756233,
    '202504': 1759238, '202505': 1763332, '202506': 1771993,
    '202507': 1778015, '202508': 1783201, '202509': 1789956,
    '202510': 1793187, '202511': 1793187
}

post_login_values = ["OTP|S", "VB|S", "TPIN|S"]

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def read_csv(path, id_col):
    """Read CSV only if exists and clean ID column"""
    if not hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path)):
        return None

    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
        .filter(F.col(id_col).isNotNull() & (F.trim(F.col(id_col)) != ""))
    )


def normalize(df):
    return df.toDF(*[c.replace(" ", "_").replace("(", "").replace(")", "") for c in df.columns])


def add_result(lst, month, channel, value):
    if value > 0:
        lst.append((month, channel, value))


# --------------------------------------------------
# FINAL RESULT CONTAINERS
# --------------------------------------------------
total_cnt   = []
post_cnt    = []
pre_cnt     = []
unique_cnt  = []
ratio_cnt   = []

# --------------------------------------------------
# MAIN PIPELINE
# --------------------------------------------------
for part, mnth in months.items():

    # ---------- STACY ----------
    stacy_all, stacy_post = None, None
    for cnt in ["en", "zh"]:
        for auth in ["pre", "post"]:
            path = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_{auth}login.csv"
            df = read_csv(path, "user_id")
            if not df:
                continue

            stacy_all = df if stacy_all is None else stacy_all.unionByName(df)
            if auth == "post":
                stacy_post = df if stacy_post is None else stacy_post.unionByName(df)

    # ---------- CALL ----------
    call_df = read_csv(
        f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv",
        "Customer No (CTI)"
    )
    if call_df:
        call_df = normalize(call_df)

    # ---------- CHAT ----------
    chat_df = read_csv(
        f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv",
        "REL ID"
    )

    # ---------- IVR ----------
    ivr_df = None
    for i in range(1, 5):
        df = read_csv(
            f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv",
            "REL_ID"
        )
        if df:
            ivr_df = df if ivr_df is None else ivr_df.unionByName(df)

    # ---------- COUNTS ----------
    cnt_stacy = stacy_all.count() if stacy_all else 0
    cnt_call  = call_df.count() if call_df else 0
    cnt_chat  = chat_df.count() if chat_df else 0
    cnt_ivr   = ivr_df.count() if ivr_df else 0

    cnt_stacy_post = stacy_post.count() if stacy_post else 0
    cnt_call_post  = call_df.filter(F.col("Verification_Status")=="Pass").count() if call_df else 0
    cnt_chat_post  = chat_df.filter(F.col("Pre/Post")=="Postlogin").count() if chat_df else 0
    cnt_ivr_post   = ivr_df.filter(F.col("ONE_FA").isin(post_login_values)).count() if ivr_df else 0

    # ---------- UNIQUE ----------
    uniq_stacy = stacy_post.select("user_id").distinct().count() if stacy_post else 0
    uniq_call  = call_df.select("Customer_No_CTI").distinct().count() if call_df else 0
    uniq_chat  = chat_df.select("REL ID").distinct().count() if chat_df else 0
    uniq_ivr   = ivr_df.select("REL_ID").distinct().count() if ivr_df else 0

    # ---------- STORE RESULTS ----------
    add_result(total_cnt, part, "Stacy", cnt_stacy)
    add_result(total_cnt, part, "Call", cnt_call)
    add_result(total_cnt, part, "Live Chat", cnt_chat)
    add_result(total_cnt, part, "IVR", cnt_ivr)

    add_result(post_cnt, part, "Stacy", cnt_stacy_post)
    add_result(post_cnt, part, "Call", cnt_call_post)
    add_result(post_cnt, part, "Live Chat", cnt_chat_post)
    add_result(post_cnt, part, "IVR", cnt_ivr_post)

    add_result(pre_cnt, part, "Stacy", cnt_stacy - cnt_stacy_post)
    add_result(pre_cnt, part, "Call", cnt_call - cnt_call_post)
    add_result(pre_cnt, part, "Live Chat", cnt_chat - cnt_chat_post)
    add_result(pre_cnt, part, "IVR", cnt_ivr - cnt_ivr_post)

    add_result(unique_cnt, part, "Stacy", uniq_stacy)
    add_result(unique_cnt, part, "Call", uniq_call)
    add_result(unique_cnt, part, "Live Chat", uniq_chat)
    add_result(unique_cnt, part, "IVR", uniq_ivr)

    hk = total_HK_clients[part]
    add_result(ratio_cnt, part, "Stacy", uniq_stacy * 100 / hk)
    add_result(ratio_cnt, part, "Call", uniq_call * 100 / hk)
    add_result(ratio_cnt, part, "Live Chat", uniq_chat * 100 / hk)
    add_result(ratio_cnt, part, "IVR", uniq_ivr * 100 / hk)

# --------------------------------------------------
# OUTPUT DATAFRAMES
# --------------------------------------------------
df_total  = spark.createDataFrame(total_cnt,  ["month","channel","volume"])
df_post   = spark.createDataFrame(post_cnt,   ["month","channel","volume"])
df_pre    = spark.createDataFrame(pre_cnt,    ["month","channel","volume"])
df_unique = spark.createDataFrame(unique_cnt, ["month","channel","volume"])
df_ratio  = spark.createDataFrame(ratio_cnt,  ["month","channel","percentage"])

df_total.show(100, False)
df_post.show(100, False)
df_pre.show(100, False)
df_unique.show(100, False)
df_ratio.show(100, False)
