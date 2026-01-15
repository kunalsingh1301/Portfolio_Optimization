from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from functools import reduce

# --------------------------------------------------
# SPARK
# --------------------------------------------------
spark = SparkSession.builder.appName("UnifiedPostloginAnalytics").getOrCreate()

# --------------------------------------------------
# MONTHS
# --------------------------------------------------
months = {
    "202501": "jan", "202502": "feb", "202503": "mar",
    "202504": "apr", "202505": "may", "202506": "jun"
}

# --------------------------------------------------
# READ HELPERS
# --------------------------------------------------
def read_call(path, mnth):
    return (
        spark.read.option("header", True).csv(path)
        .filter(col("Customer No (CTI)").isNotNull())
        .select(
            col("Customer No (CTI)").alias("user_id"),
            lit("CALL").alias("channel")
        )
    )

def read_chat(path, mnth):
    return (
        spark.read.option("header", True).csv(path)
        .filter(col("Pre/Post") == "Postlogin")
        .select(
            col("REL ID").alias("user_id"),
            lit("CHAT").alias("channel")
        )
    )

def read_stacy(path, mnth):
    return (
        spark.read.option("header", True).csv(path)
        .filter(col("Login Status") == "Postlogin")
        .select(
            col("User ID").alias("user_id"),
            lit("STACY").alias("channel")
        )
    )

# --------------------------------------------------
# LOAD ALL DATA
# --------------------------------------------------
dfs = []

for part, mnth in months.items():
    dfs.append(read_call(f"/user/data/{mnth}_call.csv", part))
    dfs.append(read_chat(f"/user/data/{mnth}_chat.csv", part))
    dfs.append(read_stacy(f"/user/data/{mnth}_stacy_en_post.csv", part))
    dfs.append(read_stacy(f"/user/data/{mnth}_stacy_zh_post.csv", part))

df = reduce(lambda a, b: a.unionByName(b), dfs)

# --------------------------------------------------
# REMOVE IVR
# --------------------------------------------------
df = df.filter(col("channel") != "IVR")

# --------------------------------------------------
# ADD ROW ORDER (NO TIMESTAMP)
# --------------------------------------------------
df = df.withColumn("row_id", monotonically_increasing_id())

# --------------------------------------------------
# USER CONTACT COUNT
# --------------------------------------------------
user_cnt = (
    df.groupBy("user_id")
      .agg(count("*").alias("total_contacts"))
)

df = df.join(user_cnt, "user_id")

# --------------------------------------------------
# FOLLOW-UP BUCKET
# --------------------------------------------------
df = df.withColumn(
    "follow_up_bucket",
    when(col("total_contacts") == 1, "follow_up_0")
    .when(col("total_contacts") == 2, "follow_up_1")
    .when(col("total_contacts") == 3, "follow_up_2")
    .otherwise("follow_up_3+")
)

# --------------------------------------------------
# STARTER CHANNEL (NO TIME)
# --------------------------------------------------
w = Window.partitionBy("user_id").orderBy("row_id")

starter = (
    df.withColumn("rn", row_number().over(w))
      .filter(col("rn") == 1)
      .select(
          "user_id",
          col("channel").alias("starter_channel")
      )
)

df = df.join(starter, "user_id", "left")

# --------------------------------------------------
# AGGREGATE PER CHANNEL
# --------------------------------------------------
final_df = (
    df.groupBy("channel")
      .agg(
          count("*").alias("total_postlogin_cases"),
          countDistinct("user_id").alias("total_distinct_users"),
          sum(when(col("follow_up_bucket") == "follow_up_0", 1).otherwise(0)).alias("follow_up_0"),
          sum(when(col("follow_up_bucket") == "follow_up_1", 1).otherwise(0)).alias("follow_up_1"),
          sum(when(col("follow_up_bucket") == "follow_up_2", 1).otherwise(0)).alias("follow_up_2"),
          sum(when(col("follow_up_bucket") == "follow_up_3+", 1).otherwise(0)).alias("follow_up_3+"),
          sum(when(col("starter_channel") == col("channel"), 1).otherwise(0)).alias("starter_customers")
      )
)

# --------------------------------------------------
# DERIVED METRICS
# --------------------------------------------------
final_df = (
    final_df
    .withColumn("total_case", col("total_postlogin_cases"))
    .withColumn(
        "rep_rate",
        (col("follow_up_1") + col("follow_up_2") + col("follow_up_3+")) / col("total_case")
    )
)

# --------------------------------------------------
# SECOND & THIRD CHANNEL LOGIC
# --------------------------------------------------
final_df = (
    final_df
    .withColumn("sec_chnl_count_1", col("follow_up_1"))
    .withColumn("sec_chnl_count_2", col("follow_up_2"))
    .withColumn("sec_chnl_count_3", col("follow_up_3+"))
    .withColumn("sec_chnl_count_4", lit(0))
    .withColumn(
        "sec_chnl_1",
        col("follow_up_1") + col("follow_up_2") + col("follow_up_3+")
    )
    .withColumn(
        "third_chnl_1",
        col("follow_up_2") + col("follow_up_3+")
    )
)

# --------------------------------------------------
# FINAL FORMAT
# --------------------------------------------------
final_df = final_df.select(
    lit("ALL").alias("Date"),
    col("channel").alias("Channel"),
    "total_postlogin_cases",
    "total_distinct_users",
    "total_case",
    "starter_customers",
    "rep_rate",
    "follow_up_0",
    "follow_up_1",
    "follow_up_2",
    col("follow_up_3+"),
    "sec_chnl_1",
    "sec_chnl_count_1",
    "sec_chnl_count_2",
    "sec_chnl_count_3",
    "third_chnl_1"
)

# --------------------------------------------------
# WRITE
# --------------------------------------------------
final_df.coalesce(6).write.mode("overwrite").option("header", True).csv(
    "/user/output/Postlogin_Channel_Analytics"
)
