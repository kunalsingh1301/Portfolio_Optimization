from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

# =========================================================
# 1. CREATE SPARK SESSION
# =========================================================
spark = SparkSession.builder \
    .appName("ChannelJourneyFullPipeline") \
    .enableHiveSupport() \
    .getOrCreate()

# Legacy parser required for mixed date formats
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# =========================================================
# 2. CONFIGURATION
# =========================================================
mnth = "aug"                 # data month
part = "202508"              # reporting month

stacy_cnt = ["en", "zh"]
stacy_auth = ["post"]
post_login_values = ["OTP|S", "VB|S", "TPIN|S"]

base_path = "/user/2030435/CallCentreAnalystics"

# =========================================================
# 3. HDFS HELPERS
# =========================================================
hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

def path_exists(p):
    return hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p))

def safe_read(reader_func, path):
    """
    Safely read a file:
    - skip missing paths
    - skip empty files
    """
    try:
        if path_exists(path):
            df = reader_func(path)
            if df is not None and not df.rdd.isEmpty():
                return df
    except Exception as e:
        print(f"Error reading {path}: {e}")
    return None

# =========================================================
# 4. TIMESTAMP NORMALIZATION
#    - CALL has special logic
#    - Others use generic logic
# =========================================================
def normalize_timestamp(df, ts_col):

    # -----------------------------------------------------
    # 4A. CALL START TIME (SPECIAL CASE)
    # -----------------------------------------------------
    if ts_col == "Call Start Time":

        # Step 1: normalize spaces (double spaces, NBSP, etc.)
        df = df.withColumn(
            "_ts",
            trim(
                regexp_replace(
                    regexp_replace(col(ts_col), u"[\u00A0\u202F\u200B]", " "),
                    r"\s+",
                    " "
                )
            )
        )

        # Step 2: Excel split numeric (e.g. "45925 0.875")
        df = df.withColumn(
            "_ts",
            when(
                col("_ts").rlike(r"^\d+\s+\d+(\.\d+)?$"),
                regexp_replace(col("_ts"),
                               r"(\d+)\s+(\d+(\.\d+)?)$",
                               r"\1.\2")
            ).otherwise(col("_ts"))
        )

        # Step 3: Excel numeric → timestamp
        df = df.withColumn(
            "_ts",
            when(
                col("_ts").rlike(r"^\d+(\.\d+)?$"),
                from_unixtime(
                    unix_timestamp(lit("1899-12-30")) +
                    col("_ts").cast("double") * 86400
                )
            ).otherwise(col("_ts"))
        )

        # Step 4: add missing seconds
        df = df.withColumn(
            "_ts",
            when(
                col("_ts").rlike(
                    r"^\d{1,2}-\d{1,2}-\d{2,4} \d{1,2}:\d{2}( AM| PM)?$"
                ),
                concat(col("_ts"), lit(":00"))
            ).otherwise(col("_ts"))
        )

        # Step 5: parse with known formats
        formats = [
            "d-M-yy HH:mm:ss", "d-M-yyyy HH:mm:ss",
            "d-M-yy hh:mm:ss a", "d-M-yyyy hh:mm:ss a",
            "M-d-yy HH:mm:ss", "M-d-yyyy HH:mm:ss",
            "M-d-yy hh:mm:ss a", "M-d-yyyy hh:mm:ss a",
            "dd/MM/yyyy HH:mm"
        ]

        df = df.withColumn(
            "event_ts",
            coalesce(*[to_timestamp(col("_ts"), f) for f in formats])
        )

        return df.drop("_ts")

    # -----------------------------------------------------
    # 4B. ALL OTHER CHANNELS
    # -----------------------------------------------------
    df = df.withColumn(
        "_raw_ts",
        trim(regexp_replace(col(ts_col), r"\s+", " "))
    )

    # Excel split numeric
    df = df.withColumn(
        "_excel_days",
        when(col("_raw_ts").rlike(r"^\d+\s+\d*\.\d+$"),
             split(col("_raw_ts"), r"\s+")[0].cast("double"))
    )

    df = df.withColumn(
        "_excel_fraction",
        when(col("_excel_days").isNotNull(),
             split(col("_raw_ts"), r"\s+")[1].cast("double"))
    )

    df = df.withColumn(
        "_excel_value",
        when(col("_excel_days").isNotNull(),
             col("_excel_days") + col("_excel_fraction"))
    )

    df = df.withColumn(
        "_excel_ts",
        when(
            col("_excel_value").isNotNull(),
            expr("""
                timestampadd(
                    SECOND,
                    cast((_excel_value - 25569) * 86400 as int),
                    timestamp('1970-01-01')
                )
            """)
        )
    )

    # String formats
    formats = [
        "d-M-yy HH:mm:ss", "d-M-yyyy HH:mm:ss",
        "d-M-yy hh:mm:ss a", "d-M-yyyy hh:mm:ss a",
        "M-d-yy HH:mm:ss", "M-d-yyyy HH:mm:ss",
        "M-d-yy hh:mm:ss a", "M-d-yyyy hh:mm:ss a",
        "dd/MM/yyyy hh:mm a", "dd/MM/yyyy hh:mm:ss a",
        "M/d/yyyy hh:mm a", "MM/dd/yyyy hh:mm a"
    ]

    df = df.withColumn(
        "_string_ts",
        coalesce(*[to_timestamp(col("_raw_ts"), f) for f in formats])
    )

    df = df.withColumn(
        "event_ts",
        when(col("_excel_ts").isNotNull(), col("_excel_ts"))
        .otherwise(col("_string_ts"))
    )

    return df.drop(
        "_raw_ts", "_excel_days", "_excel_fraction",
        "_excel_value", "_excel_ts", "_string_ts"
    )

# =========================================================
# 5. DATA READERS (STANDARD SCHEMA)
# =========================================================
def read_stacy(path):
    df = spark.read.option("header", True).csv(path)
    ts_col = "HKT" if "HKT" in df.columns else "date (UTC)"
    user_col = "user_id" if "user_id" in df.columns else "customer_id"
    df = normalize_timestamp(df, ts_col)
    return df.select(
        col(user_col).alias("user_id"),
        "event_ts",
        lit("Stacy").alias("channel")
    )

def read_ivr(path):
    df = spark.read.option("header", True).csv(path)
    df = df.filter(col("ONE_FA").isin(post_login_values))
    df = normalize_timestamp(df, "STARTTIME")
    return df.select(
        col("REL_ID").alias("user_id"),
        "event_ts",
        lit("IVR").alias("channel")
    )

def read_call(path):
    df = spark.read.option("header", True).csv(path)

    # Drop invalid CTI
    df = df.filter(
        col("Customer No (CTI)").isNotNull() &
        (trim(col("Customer No (CTI)")) != "")
    )

    df = normalize_timestamp(df, "Call Start Time")

    return df.select(
        col("Customer No (CTI)").alias("user_id"),
        "event_ts",
        lit("Call").alias("channel")
    )

def read_chat(path):
    df = spark.read.option("header", True).csv(path)
    df = df.filter(col("Pre/Post") == "Postlogin")
    df = df.withColumn("_ts", concat_ws(" ", col("Date7"), col("StartTime")))
    df = normalize_timestamp(df, "_ts")
    return df.select(
        col("REL ID").alias("user_id"),
        "event_ts",
        lit("Chat").alias("channel")
    )

# =========================================================
# 6. LOAD ALL DATASETS
# =========================================================
dfs = []

for cnt in stacy_cnt:
    for auth in stacy_auth:
        p = f"{base_path}/{mnth}_stacy_{cnt}_{auth}login.csv"
        df = safe_read(read_stacy, p)
        if df: dfs.append(df)

for i in range(1, 5):
    p = f"{base_path}/{mnth}_ivr{i}.csv"
    df = safe_read(read_ivr, p)
    if df: dfs.append(df)

df = safe_read(read_call, f"{base_path}/{mnth}_call.csv")
if df: dfs.append(df)

df = safe_read(read_chat, f"{base_path}/{mnth}_chat.csv")
if df: dfs.append(df)

if not dfs:
    raise ValueError("No data found")

# =========================================================
# 7. UNION ALL CHANNELS
# =========================================================
combined_df = (
    reduce(lambda a, b: a.unionByName(b), dfs)
    .dropna(subset=["user_id", "event_ts"])
    .repartition("user_id")
)

# =========================================================
# 8. USER JOURNEY ORDERING
# =========================================================
w = Window.partitionBy("user_id").orderBy("event_ts")

journey_df = combined_df.withColumn(
    "rn",
    row_number().over(w)
)

starter_df = journey_df.filter(col("rn") == 1) \
    .select("user_id", col("channel").alias("starter_channel"))

journey_df = journey_df.join(starter_df, "user_id")

# =========================================================
# 9. WRITE SINGLE DEBUG CSV (FULL JOURNEY)
# =========================================================
journey_df.select(
    "user_id",
    "channel",
    "event_ts",
    "rn",
    "starter_channel"
).orderBy("user_id", "rn") \
 .coalesce(1) \
 .write.mode("overwrite") \
 .option("header", True) \
 .csv(f"{base_path}/debug_full_journey_{mnth}")
