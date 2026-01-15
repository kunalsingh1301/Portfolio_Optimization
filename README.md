from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

# ---------------- SPARK SESSION ----------------
spark = SparkSession.builder.appName("xxxx").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ---------------- CONFIG ----------------
mnth = "jan"
part = "202501"

stacy_cnt = ["en", "zh"]
stacy_auth = ["post"]
post_login_values = ["OTP|S", "VB|S", "TPIN|S"]

# ---------------- HDFS HELPERS ----------------
hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)
def path_exists(p):
    return hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p))

# ---------------- TIMESTAMP NORMALIZER ----------------
def normalize_timestamp(df, ts_col):
    if ts_col == "Call Start Time":
      df = df.withColumn("_ts", regexp_replace(trim(col(ts_col)),r"\s+"," "))
    
      # Step 1: Handle space-separated numeric Excel values (e.g., "45231 0.5625")
      df = df.withColumn(
          "_ts",
          when(
              col("_ts").rlike(r"^\d+\s+\d+(\.\d+)?$"),
              regexp_replace(col("_ts"), r"(\d+)\s+(\d+(\.\d+)?)$",r"\1.\2")
          ).otherwise(col("_ts"))
      )
      
      # Step 2: Convert Excel numeric dates to timestamp (e.g., "45231.5625")
      df = df.withColumn(
          "_ts",
          when(
              col("_ts").rlike(r"^\d+(\.\d+)?$"),
              from_unixtime(unix_timestamp(lit("1899-12-30"))+(col("_ts").cast("double")*86400))
          ).otherwise(col("_ts"))
      )
      
      # Step 3: Add missing ":00" seconds to timestamps
      # Only add ":00" if the timestamp has HH:MM format (one colon) but no seconds
      df = df.withColumn(
          "_ts",
          when(
              # Match: date + space + HH:MM (no AM/PM, no seconds)
              # Pattern ensures exactly one colon in the time part
              col("_ts").rlike(r"^\d{1,2}-\d{1,2}-\d{2,4}\s+\d{1,2}:\d{2}$"),
              concat(col("_ts"), lit(":00"))
          ).when(
              # Match: date + space + HH:MM AM/PM (no seconds)
              # The space before AM/PM prevents matching if there's already ":00 AM"
              col("_ts").rlike(r"^\d{1,2}-\d{1,2}-\d{2,4}\s+\d{1,2}:\d{2}\s(AM|PM)$"),
              concat(col("_ts"), lit(":00"))
          ).otherwise(col("_ts"))
      )
  
      # Step 4: Parse timestamp using multiple format patterns
      formats = [
          # DD-MM-YYYY patterns (important for days 13-31)
          "dd-MM-yyyy HH:mm:ss", "dd-MM-yyyy hh:mm:ss a",
          "dd-MM-yyyy HH:mm", "dd-MM-yyyy hh:mm a",
          "dd-MM-yy HH:mm:ss", "dd-MM-yy hh:mm:ss a",
          "dd-MM-yy HH:mm", "dd-MM-yy hh:mm a",
          
          # D-M-YYYY patterns (flexible day/month)
          "d-M-yyyy HH:mm:ss", "d-M-yyyy hh:mm:ss a",
          "d-M-yyyy HH:mm", "d-M-yyyy hh:mm a",
          "d-M-yy HH:mm:ss", "d-M-yy hh:mm:ss a",
          "d-M-yy HH:mm", "d-M-yy hh:mm a",
          
          # Slash-based formats
          "dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy hh:mm:ss a",
          "dd/MM/yyyy HH:mm", "dd/MM/yyyy hh:mm a",
          "dd/MM/yy HH:mm:ss", "dd/MM/yy hh:mm:ss a",
          "dd/MM/yy HH:mm", "dd/MM/yy hh:mm a",
          
          # MM-DD-YYYY patterns (month-first, US format)
          "M-d-yyyy HH:mm:ss", "M-d-yyyy hh:mm:ss a",
          "M-d-yyyy HH:mm", "M-d-yyyy hh:mm a",
          "M-d-yy HH:mm:ss", "M-d-yy hh:mm:ss a",
          "M-d-yy HH:mm", "M-d-yy hh:mm a",
          
          # More slash formats
          "M/d/yyyy HH:mm:ss", "MM/dd/yyyy HH:mm:ss",
          "M/d/yyyy HH:mm", "MM/dd/yyyy HH:mm",
          "M/d/yyyy hh:mm:ss a", "MM/dd/yyyy hh:mm:ss a",
          "M/d/yyyy hh:mm a", "MM/dd/yyyy hh:mm a",
          "M/d/yy HH:mm:ss", "MM/dd/yy HH:mm:ss",
          "M/d/yy HH:mm", "MM/dd/yy HH:mm",
          "M/d/yy hh:mm:ss a", "MM/dd/yy hh:mm:ss a",
          "M/d/yy hh:mm a", "MM/dd/yy hh:mm a",
          
          # Date-only formats
          "M/d/yy", "MM/dd/yy", "M/d/yyyy", "MM/dd/yyyy"
      ]
  
      df = df.withColumn(
          "event_ts",
          coalesce(*[to_timestamp(col("_ts"), f) for f in formats])
      )
  
      return df.drop("_ts")
      
    else:
      df = df.withColumn("_raw_ts", regexp_replace(trim(col(ts_col)),r"\s+"," "))
      
      # Excel numeric handling
      df = df.withColumn("_excel_days",
                         when(col("_raw_ts").rlike(r"^\d+\s+\d*\.\d+$"),
                              split(col("_raw_ts"), r"\s+")[0].cast("double")))
      df = df.withColumn("_excel_fraction",
                         when(col("_excel_days").isNotNull(),
                              split(col("_raw_ts"), r"\s+")[1].cast("double")))
      df = df.withColumn("_excel_value",
                         when(col("_excel_days").isNotNull(), col("_excel_days")+col("_excel_fraction")))
      df = df.withColumn("_excel_ts",
                         when(col("_excel_value").isNotNull(),
                              expr("timestampadd(SECOND,cast((_excel_value-25569)*86400 as int), timestamp('1970-01-01'))")))
      
      # String timestamp formats
      formats = [
          # DD-MM-YYYY patterns
          "dd-MM-yyyy HH:mm:ss", "dd-MM-yyyy hh:mm:ss a",
          "dd-MM-yyyy HH:mm", "dd-MM-yyyy hh:mm a",
          "dd-MM-yy HH:mm:ss", "dd-MM-yy hh:mm:ss a",
          "dd-MM-yy HH:mm", "dd-MM-yy hh:mm a",
          
          # D-M-YYYY patterns
          "d-M-yyyy HH:mm:ss", "d-M-yyyy hh:mm:ss a",
          "d-M-yyyy HH:mm", "d-M-yyyy hh:mm a",
          "d-M-yy HH:mm:ss", "d-M-yy hh:mm:ss a",
          "d-M-yy HH:mm", "d-M-yy hh:mm a",
          
          # Slash formats
          "dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy hh:mm:ss a",
          "dd/MM/yyyy HH:mm", "dd/MM/yyyy hh:mm a",
          "dd/MM/yy HH:mm:ss", "dd/MM/yy hh:mm:ss a",
          "dd/MM/yy HH:mm", "dd/MM/yy hh:mm a",
          
          # US formats
          "M-d-yyyy HH:mm:ss", "M-d-yyyy hh:mm:ss a",
          "M-d-yyyy HH:mm", "M-d-yyyy hh:mm a",
          "M-d-yy HH:mm:ss", "M-d-yy hh:mm:ss a",
          "M-d-yy HH:mm", "M-d-yy hh:mm a",
          
          "M/d/yyyy hh:mm:ss a", "MM/dd/yyyy hh:mm:ss a",
          "M/d/yyyy hh:mm a", "MM/dd/yyyy hh:mm a",
          "M/d/yy hh:mm:ss a", "MM/dd/yy hh:mm:ss a",
          "M/d/yy hh:mm a", "MM/dd/yy hh:mm a",
          
          "M/d/yy", "MM/dd/yy", "M/d/yyyy", "MM/dd/yyyy"
      ]
  
      df = df.withColumn("_string_ts", coalesce(*[to_timestamp(col("_raw_ts"), f) for f in formats]))
  
      df = df.withColumn("event_ts",
                         when(col("_excel_ts").isNotNull(), col("_excel_ts"))
                         .otherwise(col("_string_ts")))

      return df


# ---------------- SAFE READER ----------------
def safe_read(func, path):
    try:
        if path_exists(path):
            df = func(path)
            if df is not None and not df.rdd.isEmpty():
                return df
        else:
            print(path + " doesn't exist")
    except Exception as e:
        print(f"Error reading {path}: {e}")
    return None

# ---------------- DATA READERS ----------------
def read_stacy(path):
    df = spark.read.option("header", True).csv(path)
    ts_col = "HKT" if "HKT" in df.columns else "date (UTC)"
    user_col = "user_id" if "user_id" in df.columns else "customer_id"
    df = df.filter(col(user_col).isNotNull()&(trim(col(user_col)) != ""))
    print("Stacy")
    print(df.count())
    df = normalize_timestamp(df, ts_col)
    return df.select(col(user_col).alias("user_id"),
                     "event_ts",
                     lit("Stacy").alias("channel"))

def read_ivr(path):
    df = spark.read.option("header", True).csv(path)
    df = df.filter(col("ONE_FA").isin(post_login_values))
    df = df.filter(col("REL_ID").isNotNull()&(trim(col("REL_ID")) != ""))
    print("ivr")
    print(df.count())
    df = normalize_timestamp(df, "STARTTIME")
    return df.select(col("REL_ID").alias("user_id"),
                     "event_ts",
                     lit("IVR").alias("channel"))

def read_call(path, debug_path=None):
    df = spark.read.option("header", True).csv(path)
    df=df.filter(
      col("Customer No (CTI)").isNotNull()&(trim(col("Customer No (CTI)")) != "")
      )
    df = df.withColumn("raw_ts",col("Call Start Time"))
    df = df.filter(df["Verification Status"] == "Pass")
    print("call")
    print(df.count())
    
    df = normalize_timestamp(df, "Call Start Time")
    df.select( col("Customer No (CTI)").alias("user_id"),"raw_ts","event_ts").coalesce(1).write.mode("overwrite").csv(f"/user/2030435/CallCentreAnalystics/0001.csv")

    return df.select(col("Customer No (CTI)").alias("user_id"),
                     "event_ts",
                     lit("Call").alias("channel"))

def read_chat(path):
    df = spark.read.option("header", True).csv(path)
    df = df.filter(col("Pre/Post") == "Postlogin")
    df = df.withColumn("_ts", concat_ws(" ", col("Date7"), col("StartTime")))
    df = df.filter(col("REL ID").isNotNull()&(trim(col("REL ID")) != ""))
    print("chat")
    print(df.count())
    df = normalize_timestamp(df, "_ts")
    return df.select(col("REL ID").alias("user_id"),
                     "event_ts",
                     lit("Chat").alias("channel"))

# ---------------- LOAD DATA ----------------
dfs = []

for cnt in stacy_cnt:
    for auth in stacy_auth:
        p = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_{auth}login.csv"
        df = safe_read(read_stacy, p)
        if df: dfs.append(df)

for i in range(1, 5):
    p = f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv"
    df = safe_read(read_ivr, p)
    if df: dfs.append(df)

# Call with debug CSV
call_debug_path = f"/user/2030435/CallCentreAnalystics/debug_call_{mnth}.csv"
df = safe_read(read_call,
               f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv")
if df: dfs.append(df)

df = safe_read(read_chat, f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv")
if df: dfs.append(df)

if not dfs:
    raise ValueError("No data found")

combined_df = reduce(lambda a, b: a.unionByName(b), dfs) \
    .dropna(subset=["user_id", "event_ts"]) \
    .repartition("user_id") \
    .cache()

total_postlogin_cases = combined_df.count()
total_distinct_users = combined_df.select(col("user_id")).distinct().count()

# ============================================================
# 1️⃣ MONTHLY FIRST CONTACT (CUSTOMER LEVEL)
# ============================================================
month_w = Window.partitionBy("user_id").orderBy("event_ts")

first_contact_df = (
    combined_df
    .withColumn("rn_month", row_number().over(month_w))
    .filter(col("rn_month") == 1)
    .groupBy("channel")
    .agg(countDistinct("user_id").alias("first_contact_customers"))
)

# ============================================================
# 2️⃣ CASE CONSTRUCTION (CASE LEVEL)
# ============================================================
w_user = Window.partitionBy("user_id").orderBy("event_ts")

df = combined_df.withColumn("rn", row_number().over(w_user))

starter_df = (
    df.filter(col("rn") == 1)
      .select("user_id", col("channel").alias("starter_channel"))
)

df = df.join(starter_df, "user_id")

df = df.withColumn(
    "case_id",
    sum(when(col("rn") == 1, 1).otherwise(0)).over(w_user)
)

# ============================================================
# 3️⃣ CASE SIZE
# ============================================================
case_df = (
    df.groupBy("starter_channel", "user_id", "case_id")
      .agg(count("*").alias("case_size"))
)

# ============================================================
# 4️⃣ TOTAL CASES + CUSTOMERS PER STARTER CHANNEL
# ============================================================
total_case_df = (
    case_df.groupBy("starter_channel")
    .agg(
        count("*").alias("total_case"),
        countDistinct("user_id").alias("starter_customers")
    )
)

# ============================================================
# 5️⃣ FOLLOW-UP BUCKETS (CASE LEVEL)
# ============================================================
bucket_df = case_df.withColumn(
    "bucket",
    when(col("case_size") == 1, "0")
    .when(col("case_size") == 2, "1")
    .when(col("case_size") == 3, "2")
    .otherwise("3+")
)

bucket_pivot = (
    bucket_df.groupBy("starter_channel", "bucket")
    .count()
    .groupBy("starter_channel")
    .pivot("bucket", ["0", "1", "2", "3+"])
    .sum("count")
    .fillna(0)
    .withColumnRenamed("0", "follow_up_0")
    .withColumnRenamed("1", "follow_up_1")
    .withColumnRenamed("2", "follow_up_2")
    .withColumnRenamed("3+", "follow_up_3+")
)

# ============================================================
# 6️⃣ REPEAT RATE (CUSTOMER LEVEL – CORRECTED)
# ============================================================
cust_case_cnt = (
    case_df.groupBy("starter_channel", "user_id")
    .agg(count("*").alias("cases_started"))
)

repeat_df = (
    cust_case_cnt.filter(col("cases_started") > 1)
    .groupBy("starter_channel")
    .agg(countDistinct("user_id").alias("repeat_customers"))
)

rep_df = (
    total_case_df
    .join(repeat_df, "starter_channel", "left")
    .fillna(0)
    .withColumn("rep_rate", col("repeat_customers") * 100 / col("starter_customers"))
)

# ============================================================
# 7️⃣ 2ND / 3RD CONTACT CHANNEL (CASE LEVEL)
# ============================================================
def top_channel(df, rn, prefix):
    base = (
        df.filter(col("rn") == rn)
        .groupBy("starter_channel", "channel")
        .agg(countDistinct(struct("user_id", "case_id")).alias("case_count"))
    )
    w = Window.partitionBy("starter_channel").orderBy(col("case_count").desc())
    ranked = base.withColumn("rank", row_number().over(w)).filter(col("rank") <= 4)

    exprs = []
    for i in range(1, 5):
        exprs += [
            max(when(col("rank") == i, col("channel"))).alias(f"{prefix}_chnl_{i}"),
            max(when(col("rank") == i, col("case_count"))).alias(f"{prefix}_chnl_count_{i}")
        ]

    return ranked.groupBy("starter_channel").agg(*exprs)

sec_df = top_channel(df, 2, "sec")
third_df = top_channel(df, 3, "third")

# ============================================================
# 8️⃣ FINAL OUTPUT
# ============================================================
final_df = (
    rep_df
    .join(bucket_pivot, "starter_channel")
    .join(sec_df, "starter_channel", "left")
    .join(third_df, "starter_channel", "left")
    .withColumnRenamed("starter_channel", "Channel")
    .withColumn("Date", lit(part))
    .withColumn("total_postlogin_cases", lit(total_postlogin_cases))
    .withColumn("total_distinct_users", lit(total_distinct_users))
)
columns = [
    "Date", "Channel","total_postlogin_cases","total_distinct_users", "total_case", "starter_customers", "rep_rate",
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

# ---------------- SHOW RESULT ----------------
final_df.show(truncate=False)
