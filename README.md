from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

# =====================================================
# SPARK
# =====================================================
spark = SparkSession.builder \
    .appName("PostLogin_CaseFlow_WithTimestamp") \
    .enableHiveSupport() \
    .getOrCreate()

# =====================================================
# CONFIG
# =====================================================
mnth = "jan"
part = "202501"
stacy_cnt = ["en", "zh"]

# =====================================================
# HDFS HELPER
# =====================================================
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
def path_exists(p):
    return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p))

def safe_read(func, path):
    if path_exists(path):
        df = func(path)
        if df is not None and not df.rdd.isEmpty():
            return df
    return None

# =====================================================
# NORMALIZE TIMESTAMP
# =====================================================
def normalize_timestamp(df, ts_col):
    # (Use the exact function you provided)
    # Handles Call Start Time, Excel numeric values, multiple formats, etc.
    # Returns df with standardized "event_ts"
    if ts_col == "Call Start Time":
        df = df.withColumn("_ts", regexp_replace(trim(col(ts_col)), r"\s+", " "))
        df = df.withColumn("_ts",
                           when(col("_ts").rlike(r"^\d+\s+\d+(\.\d+)?$"),
                                regexp_replace(col("_ts"), r"(\d+)\s+(\d+(\.\d+)?)$", r"\1.\2")
                           ).otherwise(col("_ts")))
        df = df.withColumn("_ts",
                           when(col("_ts").rlike(r"^\d+(\.\d+)?$"),
                                from_unixtime(unix_timestamp(lit("1899-12-30")) + (col("_ts").cast("double") * 86400))
                           ).otherwise(col("_ts")))
        df = df.withColumn("_ts",
                           when(col("_ts").rlike(r"^\d{1,2}-\d{1,2}-\d{2,4}\s+\d{1,2}:\d{2}$"),
                                concat(col("_ts"), lit(":00"))
                           ).otherwise(col("_ts")))
        formats = [
            "dd-MM-yyyy HH:mm:ss","dd-MM-yyyy hh:mm:ss a","dd-MM-yyyy HH:mm","dd-MM-yyyy hh:mm a",
            "dd-MM-yy HH:mm:ss","dd-MM-yy hh:mm:ss a","dd-MM-yy HH:mm","dd-MM-yy hh:mm a",
            "d-M-yyyy HH:mm:ss","d-M-yyyy hh:mm:ss a","d-M-yyyy HH:mm","d-M-yyyy hh:mm a",
            "d-M-yy HH:mm:ss","d-M-yy hh:mm:ss a","d-M-yy HH:mm","d-M-yy hh:mm a",
            "dd/MM/yyyy HH:mm:ss","dd/MM/yyyy hh:mm:ss a","dd/MM/yyyy HH:mm","dd/MM/yyyy hh:mm a",
            "dd/MM/yy HH:mm:ss","dd/MM/yy hh:mm:ss a","dd/MM/yy HH:mm","dd/MM/yy hh:mm a",
            "M-d-yyyy HH:mm:ss","M-d-yyyy hh:mm:ss a","M-d-yyyy HH:mm","M-d-yyyy hh:mm a",
            "M-d-yy HH:mm:ss","M-d-yy hh:mm:ss a","M-d-yy HH:mm","M-d-yy hh:mm a",
            "M/d/yyyy HH:mm:ss","MM/dd/yyyy HH:mm:ss","M/d/yyyy HH:mm","MM/dd/yyyy HH:mm",
            "M/d/yyyy hh:mm:ss a","MM/dd/yyyy hh:mm:ss a","M/d/yyyy hh:mm a","MM/dd/yyyy hh:mm a",
            "M/d/yy HH:mm:ss","MM/dd/yy HH:mm:ss","M/d/yy HH:mm","MM/dd/yy HH:mm",
            "M/d/yy hh:mm:ss a","MM/dd/yy hh:mm:ss a","M/d/yy hh:mm a","MM/dd/yy hh:mm a",
            "M/d/yy","MM/dd/yy","M/d/yyyy","MM/dd/yyyy"
        ]
        df = df.withColumn("event_ts", coalesce(*[to_timestamp(col("_ts"), f) for f in formats]))
        return df.drop("_ts")
    else:
        df = df.withColumn("_raw_ts", regexp_replace(trim(col(ts_col)), r"\s+", " "))
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
        formats = [
            "dd-MM-yyyy HH:mm:ss","dd-MM-yyyy hh:mm:ss a","dd-MM-yyyy HH:mm","dd-MM-yyyy hh:mm a",
            "dd-MM-yy HH:mm:ss","dd-MM-yy hh:mm:ss a","dd-MM-yy HH:mm","dd-MM-yy hh:mm a",
            "d-M-yyyy HH:mm:ss","d-M-yyyy hh:mm:ss a","d-M-yyyy HH:mm","d-M-yyyy hh:mm a",
            "d-M-yy HH:mm:ss","d-M-yy hh:mm:ss a","d-M-yy HH:mm","d-M-yy hh:mm a",
            "dd/MM/yyyy HH:mm:ss","dd/MM/yyyy hh:mm:ss a","dd/MM/yyyy HH:mm","dd/MM/yyyy hh:mm a",
            "dd/MM/yy HH:mm:ss","dd/MM/yy hh:mm:ss a","dd/MM/yy HH:mm","dd/MM/yy hh:mm a",
            "M-d-yyyy HH:mm:ss","M-d-yyyy hh:mm:ss a","M-d-yyyy HH:mm","M-d-yyyy hh:mm a",
            "M-d-yy HH:mm:ss","M-d-yy hh:mm:ss a","M-d-yy HH:mm","M-d-yy hh:mm a",
            "M/d/yyyy hh:mm:ss a","MM/dd/yyyy hh:mm:ss a","M/d/yyyy hh:mm a","MM/dd/yyyy hh:mm a",
            "M/d/yy hh:mm:ss a","MM/dd/yy hh:mm:ss a","M/d/yy hh:mm a","MM/dd/yy hh:mm a",
            "M/d/yy","MM/dd/yy","M/d/yyyy","MM/dd/yyyy"
        ]
        df = df.withColumn("_string_ts", coalesce(*[to_timestamp(col("_raw_ts"), f) for f in formats]))
        df = df.withColumn("event_ts",
                           when(col("_excel_ts").isNotNull(), col("_excel_ts"))
                           .otherwise(col("_string_ts")))
        return df

# =====================================================
# READERS WITH TIMESTAMP
# =====================================================
def read_stacy_ts(path):
    df = spark.read.option("header", True).csv(path)
    user_col = "user_id" if "user_id" in df.columns else "customer_id"
    ts_col = "HKT" if "HKT" in df.columns else "date (UTC)"
    df = df.filter(col(user_col).isNotNull())
    return normalize_timestamp(df.select(col(user_col).alias("user_id"), ts_col), ts_col) \
           .withColumn("channel", lit("Stacy"))

def read_chat_ts(path):
    df = spark.read.option("header", True).csv(path)
    df = df.filter((col("Pre/Post")=="Postlogin") & col("REL ID").isNotNull())
    df = df.withColumn("_ts", concat_ws(" ", col("Date7"), col("StartTime")))
    return normalize_timestamp(df.select(col("REL ID").alias("user_id"), "_ts"), "_ts") \
           .withColumn("channel", lit("Chat"))

def read_call_ts(path):
    df = spark.read.option("header", True).csv(path)
    df = df.filter((col("Verification Status")=="Pass") & col("Customer No (CTI)").isNotNull())
    return normalize_timestamp(df.select(col("Customer No (CTI)").alias("user_id"), "Call Start Time"), "Call Start Time") \
           .withColumn("channel", lit("Call"))

# =====================================================
# LOAD DATA
# =====================================================
dfs = []
for c in stacy_cnt:
    p = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{c}_postlogin.csv"
    df = safe_read(read_stacy_ts, p)
    if df: dfs.append(df)

df = safe_read(read_chat_ts, f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv")
if df: dfs.append(df)

df = safe_read(read_call_ts, f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv")
if df: dfs.append(df)

combined_df = reduce(lambda a,b: a.unionByName(b), dfs).dropna(subset=["user_id","event_ts"]).cache()

# =====================================================
# GLOBAL TOTALS
# =====================================================
total_postlogin_cases = combined_df.count()
total_distinct_users = combined_df.select("user_id").distinct().count()

# =====================================================
# CASE LEVEL (WITH TIMESTAMP)
# =====================================================
w = Window.partitionBy("user_id").orderBy("event_ts")
df = combined_df.withColumn("rn", row_number().over(w))
starter_df = df.filter(col("rn")==1).select("user_id", col("channel").alias("starter_channel"))
df = df.join(starter_df,"user_id")
df = df.withColumn("case_id", sum(when(col("rn")==1,1).otherwise(0)).over(w))

# =====================================================
# CASE SIZE
# =====================================================
case_df = df.groupBy("starter_channel","user_id","case_id").agg(count("*").alias("case_size"))

# =====================================================
# TOTAL CASES + STARTER CUSTOMERS
# =====================================================
total_case_df = case_df.groupBy("starter_channel").agg(
    count("*").alias("total_case"),
    countDistinct("user_id").alias("starter_customers")
)

# =====================================================
# FOLLOW-UP BUCKETS
# =====================================================
bucket_df = case_df.withColumn("bucket",
        when(col("case_size")==1,"0")
        .when(col("case_size")==2,"1")
        .when(col("case_size")==3,"2")
        .otherwise("3+")
    )
bucket_pivot = (bucket_df.groupBy("starter_channel","bucket")
    .count()
    .groupBy("starter_channel")
    .pivot("bucket", ["0","1","2","3+"])
    .sum("count")
    .fillna(0)
    .withColumnRenamed("0","follow_up_0")
    .withColumnRenamed("1","follow_up_1")
    .withColumnRenamed("2","follow_up_2")
    .withColumnRenamed("3+","follow_up_3+")
)

# =====================================================
# SECOND/THIRD CHANNEL
# =====================================================
def channel_rank(df,rn,prefix):
    base = df.filter(col("rn")==rn).groupBy("starter_channel","channel") \
             .agg(countDistinct(struct("user_id","case_id")).alias("cnt"))
    w = Window.partitionBy("starter_channel").orderBy(col("cnt").desc())
    ranked = base.withColumn("r", row_number().over(w)).filter(col("r")<=4)
    exprs=[]
    for i in range(1,5):
        exprs += [
            max(when(col("r")==i,col("channel"))).alias(f"{prefix}_chnl_{i}"),
            max(when(col("r")==i,col("cnt"))).alias(f"{prefix}_chnl_count_{i}")
        ]
    return ranked.groupBy("starter_channel").agg(*exprs)

sec_df = channel_rank(df,2,"sec")
third_df = channel_rank(df,3,"third")

# =====================================================
# FINAL OUTPUT
# =====================================================
final_df = total_case_df.join(bucket_pivot,"starter_channel") \
        .join(sec_df,"starter_channel","left") \
        .join(third_df,"starter_channel","left") \
        .withColumn("rep_rate",(col("follow_up_1")+col("follow_up_2")+col("follow_up_3+"))/col("total_case")) \
        .withColumn("Date",lit(part)) \
        .withColumn("total_postlogin_cases",lit(total_postlogin_cases)) \
        .withColumn("total_distinct_users",lit(total_distinct_users)) \
        .withColumnRenamed("starter_channel","Channel")

# =====================================================
# FINAL COLUMNS
# =====================================================
columns = [
    "Date","Channel","total_postlogin_cases","total_distinct_users",
    "total_case","starter_customers","rep_rate",
    "follow_up_0","follow_up_1","follow_up_2","follow_up_3+",
    "sec_chnl_1","sec_chnl_2","sec_chnl_3","sec_chnl_4",
    "sec_chnl_count_1","sec_chnl_count_2","sec_chnl_count_3","sec_chnl_count_4",
    "third_chnl_1","third_chnl_2","third_chnl_3","third_chnl_4",
    "third_chnl_count_1","third_chnl_count_2","third_chnl_count_3","third_chnl_count_4"
]

for c in columns:
    if c not in final_df.columns:
        final_df = final_df.withColumn(c, lit(None))

final_df.select(columns).show(truncate=False)
