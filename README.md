from pyspark.sql import SparkSession
from pyspark.sql.functions import col, format_number
from functools import reduce

# ===============================
# SPARK SESSION
# ===============================
spark = SparkSession.builder \
    .appName("CallCentreAnalytics_SingleFile") \
    .getOrCreate()

hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

# ===============================
# CONFIG
# ===============================
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

BASE_PATH = "/user/2030435/CallCentreAnalystics"

# ===============================
# HELPERS
# ===============================
def read_csv_if_exists(path):
    if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path)):
        return spark.read.option("header", "true").csv(path)
    return None

def union_all(dfs):
    dfs = [df for df in dfs if df is not None]
    if not dfs:
        return None
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)

# ===============================
# RESULT CONTAINERS
# ===============================
total_contacts = []
post_contacts = []
pre_contacts = []
unique_contacts = []
contact_ratio = []

# ===============================
# MAIN LOOP
# ===============================
for part, mnth in months.items():

    # ---------- STACY ----------
    stacy_all_dfs = []
    stacy_post_dfs = []

    for cnt in ['en', 'zh']:
        for auth in ['pre', 'post']:
            path = f"{BASE_PATH}/{mnth}_stacy_{cnt}_{auth}login.csv"
            df = read_csv_if_exists(path)
            if df:
                stacy_all_dfs.append(df)
                if auth == "post":
                    stacy_post_dfs.append(df)

    stacy_all = union_all(stacy_all_dfs)
    stacy_post = union_all(stacy_post_dfs)

    # ---------- IVR ----------
    ivr_dfs = [
        read_csv_if_exists(f"{BASE_PATH}/{mnth}_ivr{i}.csv")
        for i in range(1, 5)
    ]
    ivr_all = union_all(ivr_dfs)

    # ---------- CALL ----------
    call_df = read_csv_if_exists(f"{BASE_PATH}/{mnth}_call.csv")
    if call_df:
        call_df = call_df.toDF(*(c.replace(" ", "_") for c in call_df.columns))

    # ---------- CHAT ----------
    chat_df = read_csv_if_exists(f"{BASE_PATH}/{mnth}_chat.csv")

    # ---------- CACHE ----------
    for df in [stacy_all, stacy_post, ivr_all, call_df, chat_df]:
        if df:
            df.cache()

    # ===============================
    # TOTAL CONTACTS
    # ===============================
    if stacy_all:
        total_contacts.append((part, "Stacy", stacy_all.count()))
    if chat_df:
        total_contacts.append((part, "Live Chat", chat_df.count()))
    if call_df:
        total_contacts.append((part, "Call", call_df.count()))
    if ivr_all:
        total_contacts.append((part, "IVR", ivr_all.count()))

    # ===============================
    # POST LOGIN
    # ===============================
    if stacy_post:
        post_contacts.append((part, "Stacy", stacy_post.count()))

    if chat_df:
        post_contacts.append((
            part, "Live Chat",
            chat_df.filter(col("Pre/Post") == "Postlogin").count()
        ))

    if call_df:
        post_contacts.append((
            part, "Call",
            call_df.filter(col("Verification_Status") == "Pass").count()
        ))

    if ivr_all:
        post_contacts.append((
            part, "IVR",
            ivr_all.filter(col("ONE_FA").isin(post_login_values)).count()
        ))

    # ===============================
    # PRE LOGIN = TOTAL - POST
    # ===============================
    for _, ch, tot in [r for r in total_contacts if r[0] == part]:
        post = next((p[2] for p in post_contacts if p[0] == part and p[1] == ch), 0)
        if tot - post > 0:
            pre_contacts.append((part, ch, tot - post))

    # ===============================
    # UNIQUE CUSTOMERS
    # ===============================
    if stacy_post:
        uid_col = "user_id" if "user_id" in stacy_post.columns else "customer_id"
        unique_contacts.append((
            part, "Stacy",
            stacy_post.select(uid_col).distinct().count()
        ))

    if chat_df:
        unique_contacts.append((
            part, "Live Chat",
            chat_df.select("REL ID").distinct().count()
        ))

    if call_df:
        unique_contacts.append((
            part, "Call",
            call_df.select("Customer_No_(CTI)").distinct().count()
        ))

    if ivr_all:
        unique_contacts.append((
            part, "IVR",
            ivr_all.select("REL_ID").distinct().count()
        ))

    # ===============================
    # CONTACT RATIO
    # ===============================
    base = total_HK_clients[part]
    for _, ch, cnt in [u for u in unique_contacts if u[0] == part]:
        contact_ratio.append((part, ch, (cnt / base) * 100))

# ===============================
# FINAL DATAFRAMES
# ===============================
df_total = spark.createDataFrame(total_contacts, ["month", "channel", "volume"])
df_post = spark.createDataFrame(post_contacts, ["month", "channel", "volume"])
df_pre  = spark.createDataFrame(pre_contacts,  ["month", "channel", "volume"])
df_uniq = spark.createDataFrame(unique_contacts, ["month", "channel", "volume"])
df_ratio = spark.createDataFrame(contact_ratio, ["month", "channel", "volume"])

# ===============================
# FORMAT & SHOW
# ===============================
df_total.withColumn("formatted_volume", format_number("volume", 0)) \
    .show(100, False)

df_post.withColumn("formatted_volume", format_number("volume", 0)) \
    .show(100, False)

df_pre.withColumn("formatted_volume", format_number("volume", 0)) \
    .show(100, False)

df_uniq.withColumn("formatted_volume", format_number("volume", 0)) \
    .show(100, False)

df_ratio.withColumn("percentage", format_number("volume", 2)) \
    .show(100, False)
