from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("PostLoginFlow_StarterChannel_NoIVR") \
    .enableHiveSupport() \
    .getOrCreate()

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
mnth = "jan"
part = "202501"

stacy_cnt = ["en", "zh"]
stacy_auth = ["post"]

# --------------------------------------------------
# READERS
# --------------------------------------------------
def read_stacy(path):
    df = spark.read.option("header", True).csv(path)
    user_col = "user_id" if "user_id" in df.columns else "customer_id"
    return df.filter(col(user_col).isNotNull()) \
             .select(col(user_col).alias("user_id")) \
             .withColumn("channel", lit("Stacy"))

def read_chat(path):
    df = spark.read.option("header", True).csv(path)
    return df.filter(col("Pre/Post") == "Postlogin") \
             .filter(col("REL ID").isNotNull()) \
             .select(col("REL ID").alias("user_id")) \
             .withColumn("channel", lit("Chat"))

def read_call(path):
    df = spark.read.option("header", True).csv(path)
    return df.filter(col("Verification Status") == "Pass") \
             .filter(col("Customer No (CTI)").isNotNull()) \
             .select(col("Customer No (CTI)").alias("user_id")) \
             .withColumn("channel", lit("Call"))

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------
dfs = []
for cnt in stacy_cnt:
    for auth in stacy_auth:
        p = f"/user/2030435/CallCentreAnalystics/{mnth}_stacy_{cnt}_{auth}login.csv"
        df = read_stacy(p)
        dfs.append(df)

dfs.append(read_chat(f"/user/2030435/CallCentreAnalystics/{mnth}_chat.csv"))
dfs.append(read_call(f"/user/2030435/CallCentreAnalystics/{mnth}_call.csv"))

combined_df = reduce(lambda a,b: a.unionByName(b), dfs).cache()

# --------------------------------------------------
# TOTALS
# --------------------------------------------------
total_postlogin_cases = combined_df.count()
total_distinct_users = combined_df.select("user_id").distinct().count()

# --------------------------------------------------
# USER CONTACT COUNTS
# --------------------------------------------------
user_contact_cnt = combined_df.groupBy("user_id") \
                              .agg(count("*").alias("user_total_contacts"))
df = combined_df.join(user_contact_cnt, "user_id")

# --------------------------------------------------
# FOLLOW-UP BUCKETS
# --------------------------------------------------
df = df.withColumn(
    "follow_bucket",
    when(col("user_total_contacts") == 1, "0")
    .when(col("user_total_contacts") == 2, "1")
    .when(col("user_total_contacts") == 3, "2")
    .otherwise("3+")
)

# --------------------------------------------------
# STARTER CUSTOMERS (first contact per user)
# --------------------------------------------------
w_user = Window.partitionBy("user_id").orderBy("channel")
starter_df = df.withColumn("rn", row_number().over(w_user)) \
               .filter(col("rn") == 1) \
               .groupBy("channel") \
               .agg(countDistinct("user_id").alias("starter_customers"))

# --------------------------------------------------
# TOTAL CASES PER CHANNEL
# --------------------------------------------------
total_case_df = df.groupBy("channel").agg(count("*").alias("total_case"))

# --------------------------------------------------
# FOLLOW-UP COUNTS
# --------------------------------------------------
bucket_df = df.groupBy("channel","follow_bucket").count() \
              .groupBy("channel") \
              .pivot("follow_bucket", ["0","1","2","3+"]).sum("count") \
              .fillna(0) \
              .withColumnRenamed("0","follow_up_0") \
              .withColumnRenamed("1","follow_up_1") \
              .withColumnRenamed("2","follow_up_2") \
              .withColumnRenamed("3+","follow_up_3+")

# --------------------------------------------------
# SECOND CHANNEL (users with follow_up 1+)
# --------------------------------------------------
df2 = df.filter(col("follow_bucket").isin(["1","2","3+"]))
user_channels = df2.groupBy("user_id").agg(collect_set("channel").alias("channels_used"))
df2 = df2.join(user_channels, "user_id").withColumn("other_channels", expr("filter(channels_used, x -> x != channel)"))

second_df = df2.filter(size(col("other_channels")) >= 1) \
               .withColumn("sec_channel", col("other_channels")[0]) \
               .groupBy("channel","sec_channel").count()

# --------------------------------------------------
# THIRD CHANNEL (users with follow_up 2+)
# --------------------------------------------------
df3 = df.filter(col("follow_bucket").isin(["2","3+"]))
user_channels3 = df3.groupBy("user_id").agg(collect_set("channel").alias("channels_used"))
df3 = df3.join(user_channels3, "user_id").withColumn("other_channels", expr("filter(channels_used, x -> x != channel)"))

third_df = df3.filter(size(col("other_channels")) >= 2) \
               .withColumn("third_channel", col("other_channels")[1]) \
               .groupBy("channel","third_channel").count()

# --------------------------------------------------
# PIVOT FUNCTION
# --------------------------------------------------
def pivot_rank(df, col_name, prefix):
    w = Window.partitionBy("channel").orderBy(col("count").desc())
    ranked = df.withColumn("r", row_number().over(w)).filter(col("r") <= 4)
    exprs = []
    for i in range(1,5):
        exprs += [
            max(when(col("r")==i, col(col_name))).alias(f"{prefix}_chnl_{i}"),
            max(when(col("r")==i, col("count"))).alias(f"{prefix}_chnl_count_{i}")
        ]
    return ranked.groupBy("channel").agg(*exprs)

sec_pivot = pivot_rank(second_df, "sec_channel", "sec")
third_pivot = pivot_rank(third_df, "third_channel", "third")

# --------------------------------------------------
# FINAL ASSEMBLY
# --------------------------------------------------
final_df = total_case_df \
    .join(bucket_df, "channel") \
    .join(sec_pivot, "channel", "left") \
    .join(third_pivot, "channel", "left") \
    .join(starter_df, "channel", "left") \
    .withColumn("Date", lit(part)) \
    .withColumn("total_postlogin_cases", lit(total_postlogin_cases)) \
    .withColumn("total_distinct_users", lit(total_distinct_users)) \
    .withColumn("rep_rate", (col("follow_up_1")+col("follow_up_2")+col("follow_up_3+"))/col("total_case")) \
    .withColumnRenamed("channel","Channel")

# --------------------------------------------------
# FINAL COLUMN ORDER
# --------------------------------------------------
columns = [
    "Date", "Channel",
    "total_postlogin_cases","total_distinct_users",
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

final_df = final_df.select(columns)

# --------------------------------------------------
# SHOW RESULT
# --------------------------------------------------
final_df.show(truncate=False)
