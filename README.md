from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, row_number, when, count, countDistinct,
    concat_ws, to_timestamp, coalesce, regexp_extract,
    concat, lpad
)
from pyspark.sql.window import Window

# ---------------- SPARK SESSION ----------------
spark = SparkSession.builder.appName("ChannelFlowOptimized").getOrCreate()

# ---------------- CONFIG ----------------
mnth = "jan"
part = "202501"

stacy_cnt = ["en", "zh"]
stacy_auth = ["post"]
post_login_values = ["OTP|S", "VB|S", "TPIN|S"]
channels = ["Stacy", "IVR", "Call", "Chat"]

hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

def path_exists(p):
    return hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(p))

# ---------------- FAST + SAFE TIMESTAMP PARSER ----------------
from pyspark.sql.functions import (
    col, when, regexp_extract, concat, lit, lpad, to_timestamp, coalesce
)

def parse_timestamp(df, ts_col):

    df = df.withColumn(
        "_has_ampm",
        col(ts_col).rlike("(?i)\\s(AM|PM)$")
    )

    df = df.withColumn(
        "_hour_24",
        regexp_extract(col(ts_col), r"\s(\d{1,2}):", 1).cast("int")
    )

    df = df.withColumn(
        "_norm_ts",
        when(
            col("_has_ampm") & (col("_hour_24") > 12),
            concat(
                regexp_extract(col(ts_col), r"^(\d{1,2}-\d{1,2}-\d{2,4})", 1),
                lit(" "),
                lpad((col("_hour_24") - 12).cast("string"), 2, "0"),
                lit(":"),
                regexp_extract(col(ts_col), r":(\d{2}:\d{2}\s(?i)(AM|PM))", 1)
            )
        ).otherwise(col(ts_col))
    )

    df = df.withColumn(
        "event_ts",
        coalesce(
            to_timestamp(col("_norm_ts"), "dd-MM-yy hh:mm:ss a"),
            to_timestamp(col("_norm_ts"), "dd-MM-yyyy hh:mm:ss a"),
            to_timestamp(col("_norm_ts"), "dd-MM-yy HH:mm:ss"),
            to_timestamp(col("_norm_ts"), "dd-MM-yyyy HH:mm:ss"),
            to_timestamp(col("_norm_ts"), "dd-MM-yy HH:mm"),
            to_timestamp(col("_norm_ts"), "dd-MM-yyyy HH:mm"),
            to_timestamp(col("_norm_ts"), "dd-MM-yyyy hh:mm")
        )
    )

    return df.drop("_has_ampm", "_hour_24", "_norm_ts")

# ---------------- READERS ----------------
def read_stacy(path):
    df = spark.read.csv(path, header=True)

    ts_col = "HKT" if "HKT" in df.columns else "date (UTC)"
    user_col = "user_id" if "user_id" in df.columns else "customer_id"

    df = parse_timestamp(df, ts_col)

    return df.select(
        col(user_col).alias("user_id"),
        col("event_ts"),
        lit("Stacy").alias("channel")
    )

def read_ivr(path):
    df = spark.read.csv(path, header=True)
    df = df.filter(col("ONE_FA").isin(post_login_values))
    df = parse_timestamp(df, "STARTTIME")

    return df.select(
        col("REL_ID").alias("user_id"),
        col("event_ts"),
        lit("IVR").alias("channel")
    )

def read_call(path):
    df = spark.read.csv(path, header=True)
    df = parse_timestamp(df, "Call Start Time")

    return df.select(
        col("Customer No (CTI)").alias("user_id"),
        col("event_ts"),
        lit("Call").alias("channel")
    )

def read_chat(path):
    df = spark.read.csv(path, header=True)
    df = df.filter(col("Pre/Post") == "Postlogin")

    df = df.withColumn(
        "_ts",
        concat_ws(" ", col("Date28"), col("StartTime"))
    )

    df = parse_timestamp(df, "_ts")

    return df.select(
        col("REL ID").alias("user_id"),
        col("event_ts"),
        lit("Chat").alias("channel")
    )

# ---------------- LOAD DATA ----------------
dfs = []

for cnt in stacy_cnt:
    for auth in stacy_auth:
        p = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_{auth}login.csv"
        if path_exists(p):
            dfs.append(read_stacy(p))

for i in range(1, 5):
    p = f"/user/2030435/CallCentreAnalystics/{mnth}_ivr{i}.csv"
    if path_exists(p):
        dfs.append(read_ivr(p))

call_p = f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"
if path_exists(call_p):
    dfs.append(read_call(call_p))

chat_p = f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv"
if path_exists(chat_p):
    dfs.append(read_chat(chat_p))

combined_df = dfs[0]
for d in dfs[1:]:
    combined_df = combined_df.unionByName(d)

combined_df = combined_df.dropna(subset=["user_id", "event_ts"]).cache()
combined_df.count()  # materialize once

# ---------------- SEQUENCE ----------------
w = Window.partitionBy("user_id").orderBy("event_ts")
df = combined_df.withColumn("rn", row_number().over(w))

# ---------------- AGGREGATION ----------------
results = []

for ch in channels:
    starters = df.filter((col("rn") == 1) & (col("channel") == ch)) \
                 .select("user_id").distinct()

    scoped = df.join(starters, "user_id")

    agg = scoped.agg(
        count("*").alias("Total_case"),
        countDistinct("user_id").alias("uniq_cust")
    ).collect()[0]

    total_case = agg["Total_case"]
    uniq_cust = agg["uniq_cust"]
    rep_rate = (total_case - uniq_cust) * 100 / total_case if total_case else 0

    bucket = (
        scoped.groupBy("user_id").count()
        .withColumn(
            "bucket",
            when(col("count") == 1, "0")
            .when(col("count") == 2, "1")
            .when(col("count") == 3, "2")
            .otherwise("3+")
        )
        .groupBy("bucket").count()
    )

    bmap = {r["bucket"]: r["count"] for r in bucket.collect()}

    def top_n(n):
        rows = (
            scoped.filter(col("rn") == n)
            .groupBy("channel").count()
            .orderBy(col("count").desc())
            .limit(4).collect()
        )
        chs = [r["channel"] for r in rows] + [""] * (4 - len(rows))
        cnts = [r["count"] for r in rows] + [0] * (4 - len(rows))
        return chs, cnts

    sec_ch, sec_ct = top_n(2)
    thr_ch, thr_ct = top_n(3)

    results.append([
        part, ch, total_case, uniq_cust, rep_rate,
        bmap.get("0", 0), bmap.get("1", 0),
        bmap.get("2", 0), bmap.get("3+", 0),
        *sec_ch, *sec_ct,
        *thr_ch, *thr_ct
    ])

# ---------------- OUTPUT ----------------
columns = [
    "Date", "Channel", "Total_case", "uniq_cust", "rep_rate",
    "follow_up_0", "follow_up_1", "follow_up_2", "follow_up_3+",
    "sec_con_chnl_1", "sec_con_chnl_2", "sec_con_chnl_3", "sec_con_chnl_4",
    "sec_con_chnl_count_1", "sec_con_chnl_count_2", "sec_con_chnl_count_3", "sec_con_chnl_count_4",
    "third_con_chnl_1", "third_con_chnl_2", "third_con_chnl_3", "third_con_chnl_4",
    "third_con_chnl_count_1", "third_con_chnl_count_2", "third_con_chnl_count_3", "third_con_chnl_count_4"
]

output_df = spark.createDataFrame(results, columns)
output_df.show(truncate=False)
Py4JJavaError: An error occurred while calling o1270.count. : org.apache.spark.SparkException: Job aborted due to stage failure: Task 1 in stage 18.0 failed 4 times, most recent failure: Lost task 1.3 in stage 18.0 (TID 36) (hkigwdjs48xwn25.hk.standardchartered.com executor 1): org.apache.spark.SparkUpgradeException: You may get a different result due to the upgrading to Spark >= 3.0: Fail to parse '31-01-2025 11:01' in the new parser. You can set spark.sql.legacy.timeParserPolicy to LEGACY to restore the behavior before Spark 3.0, or set to CORRECTED and treat it as an invalid datetime string. at org.apache.spark.sql.errors.QueryExecutionErrors$.failToParseDateTimeInNewParserError(QueryExecutionErrors.scala:1083) at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:148) at org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper$$anonfun$checkParsedDiff$1.applyOrElse(DateTimeFormatterHelper.scala:141) at scala.runtime.AbstractPartialFunction.apply(AbstractPartialFunction.scala:38) at org.apache.spark.sql.catalyst.util.Iso8601TimestampFormatter.parse(TimestampFormatter.scala:176) at org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage2.processNext(Unknown Source) at org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43) at org.apache.spark.sql.execution.WholeStageCodegenExec$$anon$1.hasNext(WholeStageCodegenExec.scala:760) at org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer$$anon$1.hasNext(InMemoryRelation.scala:118) at scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460) at org.apache.spark.storage.memory.MemoryStore.putIterator(MemoryStore.scala:223) at org.apache.spark.storage.memory.MemoryStore.putIteratorAsValues(MemoryStore.scala:302) at org.apache.spark.storage.BlockManager.$anonfun$doPutIterator$1(BlockManager.scala:1523) at org.apache.spark.storage.BlockManager.org$apache$spark$storage$BlockManager$$doPut(BlockManager.scala:1450) at org.apache.spark.storage.BlockManager.doPutIterator(BlockManager.scala:1514) at org.apache.spark.storage.BlockManager.getOrElseUpdate(BlockManager.scala:1337) at org.apache.spark.rdd.RDD.getOrCompute(RDD.scala:376) at org.apache.spark.rdd.RDD.iterator(RDD.scala:327) at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52) at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365) at org.apache.spark.rdd.RDD.iterator(RDD.scala:329) at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52) at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365) at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
