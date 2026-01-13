from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

# ---------------- SPARK SESSION ----------------
spark = SparkSession.builder.appName("xxxx").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ---------------- CONFIG ----------------
mnth = "aug"
part = "202508"

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
      df = df.withColumn("_ts", trim(col(ts_col)))
    
      df = df.withColumn(
          "_ts",
          when(
              col("_ts").rlike(r"^\d+\s+\d+(\.\d+)?$"),
              regexp_replace(col("_ts"), r"(\d+)\s+(\d+(\.\d+)?)$",r"\1.\2")
          ).otherwise(col("_ts"))
      )
      
      # ---------------- Step 3: convert Excel numeric dates to timestamp ----------------
      df = df.withColumn(
          "_ts",
          when(
              col("_ts").rlike(r"^\d+(\.\d+)?$"),
              from_unixtime(unix_timestamp(lit("1899-12-30"))+(col("_ts").cast("double")*86400))
          ).otherwise(col("_ts"))
      )
      
      #adding missing 00 second
      df = df.withColumn(
          "_ts",
          when(col("_ts").rlike(r"^\d{1,2}-\d{1,2}-\d{2,4} \d{1,2}:\d{2}( AM| PM)?$"),
               concat(col("_ts"), lit(":00")))
          .otherwise(col("_ts"))
      )
      
      
  
      formats = [
          "d-M-yy HH:mm:ss", "d-M-yy hh:mm:ss a",
          "d-M-yyyy HH:mm:ss", "d-M-yyyy hh:mm:ss a",
          "d-M-yy HH:mm", "d-M-yy hh:mm a",
          "d-M-yyyy HH:mm", "d-M-yyyy hh:mm a",
          
          "M-d-yy HH:mm:ss","M-d-yyyy HH:mm:ss",
          "M-d-yy HH:mm","M-d-yyyy HH:mm",
          "M-d-yy HH:mm:ss a","M-d-yyyy HH:mm:ss a",
          "M-d-yy HH:mm a","M-d-yyyy HH:mm a",
          
          "dd/MM/yyyy HH:mm"
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
          "d-M-yy HH:mm:ss", "d-M-yy hh:mm:ss a",
          "d-M-yyyy HH:mm:ss", "d-M-yyyy hh:mm:ss a",
          "d-M-yy HH:mm", "d-M-yy hh:mm a",
          "d-M-yyyy HH:mm", "d-M-yyyy hh:mm a",
          "M-d-yy HH:mm:ss", "M-d-yyyy HH:mm:ss",
          "M-d-yy HH:mm", "M-d-yyyy HH:mm",
          "M-d-yy HH:mm:ss a", "M-d-yyyy HH:mm:ss a",
          "M-d-yy HH:mm a", "M-d-yyyy HH:mm a",
          "dd/MM/yyyy hh:mm a", "dd/MM/yyyy hh:mm:ss a",
          "dd/MM/yy hh:mm a", "dd/MM/yy hh:mm:ss a",
          "M/d/yy hh:mm a", "MM/dd/yy hh:mm a",
          "M/d/yyyy hh:mm a", "MM/dd/yyyy hh:mm a",
          "M/d/yy hh:mm:ss a", "MM/dd/yy hh:mm:ss a",
          "M/d/yyyy hh:mm:ss a", "MM/dd/yyyy hh:mm:ss a",
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
    df = normalize_timestamp(df, ts_col)
    return df.select(col(user_col).alias("user_id"),
                     "event_ts",
                     lit("Stacy").alias("channel"))

def read_ivr(path):
    df = spark.read.option("header", True).csv(path)
    df = df.filter(col("ONE_FA").isin(post_login_values))
    df = normalize_timestamp(df, "STARTTIME")
    return df.select(col("REL_ID").alias("user_id"),
                     "event_ts",
                     lit("IVR").alias("channel"))

def read_call(path, debug_path=None):
    df = spark.read.option("header", True).csv(path)
    df=df.filter(
      col("Customer No (CTI)").isNotNull()&(trim(col("Customer No (CTI)")) != "")
      )
    
    #test_df = df.withColumn("pts",to_timestamp(col("Call Start Time"),"M-d-yyyy hh:mm)
      
    #df = df.withColumn("_raw_ts", col("Call Start Time"))
    df = normalize_timestamp(df, "Call Start Time")
    #df = df.withColumn("event_ts",to_timestamp(col("Call Start Time"),"M-d-yyyy hh:mm"))

    #if debug_path:
    #    df.select(
    #        col("Customer No (CTI)").alias("user_id"),
    #        "_raw_ts",
    #        "event_ts"
    #    ).coalesce(1).write.mode("overwrite").option("header", True).csv(debug_path)
    #    print(f"Call debug CSV written to: {debug_path}")

    return df.select(col("Customer No (CTI)").alias("user_id"),
                     "event_ts",
                     lit("Call").alias("channel"))

def read_chat(path):
    df = spark.read.option("header", True).csv(path)
    df = df.filter(col("Pre/Post") == "Postlogin")
    df = df.withColumn("_ts", concat_ws(" ", col("Date7"), col("StartTime")))
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
df = safe_read(lambda path: read_call(path, debug_path=call_debug_path),
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

# ---------------- RN + STARTER CHANNEL ----------------
w = Window.partitionBy("user_id").orderBy("event_ts")
df = combined_df.withColumn("rn", row_number().over(w))
starter_df = df.filter(col("rn") == 1).select("user_id", col("channel").alias("starter_channel"))
df = df.join(starter_df, "user_id")

# ---------------- TOTAL + REP RATE ----------------
agg_df = df.groupBy("starter_channel").agg(
    count("*").alias("total_case"),
    countDistinct("user_id").alias("uniq_cust")
).withColumn(
    "rep_rate",
    (col("total_case") - col("uniq_cust")) * 100 / col("total_case")
)

# ---------------- FOLLOW-UP BUCKETS ----------------
follow_df = df.groupBy("starter_channel", "user_id").count()
bucket_df = follow_df.withColumn(
    "bucket",
    when(col("count") == 1, "0")
    .when(col("count") == 2, "1")
    .when(col("count") == 3, "2")
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

# ---------------- SECOND / THIRD CHANNEL (FIXED) ----------------
def top_contact_fixed(df, rn, prefix, top_n=4):
    nth_df = df.filter(col("rn") == rn)
    base = nth_df.groupBy("starter_channel", "channel") \
                 .agg(countDistinct("user_id").alias("user_count"))
    w = Window.partitionBy("starter_channel").orderBy(col("user_count").desc())
    ranked = base.withColumn("rank", row_number().over(w)).filter(col("rank") <= top_n)
    exprs = []
    for i in range(1, top_n + 1):
        exprs += [
            max(when(col("rank") == i, col("channel"))).alias(f"{prefix}_chnl_{i}"),
            max(when(col("rank") == i, col("user_count"))).alias(f"{prefix}_chnl_count_{i}")
        ]
    return ranked.groupBy("starter_channel").agg(*exprs)

sec_df = top_contact_fixed(df, 2, "sec")
third_df = top_contact_fixed(df, 3, "third")

# ---------------- FINAL JOIN ----------------
final_df = (
    agg_df
    .join(bucket_pivot, "starter_channel", "left")
    .join(sec_df, "starter_channel", "left")
    .join(third_df, "starter_channel", "left")
    .withColumnRenamed("starter_channel", "Channel")
    .withColumn("Date", lit(part))
)

# ---------------- FIX OUTPUT SCHEMA ----------------
columns = [
    "Date", "Channel", "total_case", "uniq_cust", "rep_rate",
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
