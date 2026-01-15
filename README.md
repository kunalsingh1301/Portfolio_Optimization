from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as Fsum, expr, size, collect_list, when
from functools import reduce

spark = SparkSession.builder.appName("PostLoginFlow_InteractionLevel").getOrCreate()

# ---------- Example combined_df schema ----------
# user_id | channel | interaction_count | event_ts

# --------------------------------------------------
# TOTAL POSTLOGIN CASES
# --------------------------------------------------
total_postlogin_cases = combined_df.select(Fsum("interaction_count")).collect()[0][0]
total_distinct_users = combined_df.select("user_id").distinct().count()

# --------------------------------------------------
# FOLLOW-UP BUCKET
# --------------------------------------------------
user_total = (
    combined_df.groupBy("user_id", "channel")
    .agg(Fsum("interaction_count").alias("total_interactions"))
)

user_total = user_total.withColumn(
    "follow_bucket",
    when(col("total_interactions") == 1, "0")
    .when(col("total_interactions") == 2, "1")
    .when(col("total_interactions") == 3, "2")
    .otherwise("3+")
)

# Pivot to get follow_up_0,1,2,3+
bucket_df = (
    user_total.groupBy("channel")
    .pivot("follow_bucket", ["0","1","2","3+"])
    .agg(Fsum("total_interactions"))
    .fillna(0)
    .withColumnRenamed("0","follow_up_0")
    .withColumnRenamed("1","follow_up_1")
    .withColumnRenamed("2","follow_up_2")
    .withColumnRenamed("3+","follow_up_3+")
)

# Total case per channel
total_case_df = user_total.groupBy("channel").agg(Fsum("total_interactions").alias("total_case"))

# --------------------------------------------------
# SECOND / THIRD CHANNEL (SUM INTERACTIONS)
# --------------------------------------------------
# Collect all other channels per user
user_channels = (
    combined_df.groupBy("user_id")
    .agg(collect_list("channel").alias("channels_used"),
         Fsum("interaction_count").alias("interaction_sum"))
)

# Explode other channels for second and third
from pyspark.sql.functions import array_except, element_at

user_channels = user_channels.withColumn(
    "second_channel",
    element_at(array_except(col("channels_used"), array(col("channels_used")[0])), 1)
).withColumn(
    "third_channel",
    element_at(array_except(col("channels_used"), array(col("channels_used")[0], col("second_channel"))), 1)
)

# Sum interactions by main channel -> second/third channel
second_df = (
    combined_df.join(user_channels, "user_id")
    .groupBy("channel", "second_channel")
    .agg(Fsum("interaction_count").alias("second_channel_sum"))
)

third_df = (
    combined_df.join(user_channels, "user_id")
    .groupBy("channel", "third_channel")
    .agg(Fsum("interaction_count").alias("third_channel_sum"))
)

# Pivot top 4
def pivot_top(df, col_name, sum_col, prefix):
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, max
    
    w = Window.partitionBy("channel").orderBy(col(sum_col).desc())
    ranked = df.withColumn("r", row_number().over(w)).filter(col(sum_col).isNotNull())
    
    exprs = []
    for i in range(1,5):
        exprs += [
            max(when(col("r") == i, col(col_name))).alias(f"{prefix}_chnl_{i}"),
            max(when(col("r") == i, col(sum_col))).alias(f"{prefix}_chnl_count_{i}")
        ]
    return ranked.groupBy("channel").agg(*exprs)

sec_pivot = pivot_top(second_df, "second_channel", "second_channel_sum", "sec")
third_pivot = pivot_top(third_df, "third_channel", "third_channel_sum", "third")

# --------------------------------------------------
# FINAL ASSEMBLY
# --------------------------------------------------
final_df = (
    total_case_df
    .join(bucket_df, "channel")
    .join(sec_pivot, "channel", "left")
    .join(third_pivot, "channel", "left")
    .withColumn("Date", lit("202510"))
    .withColumn("total_postlogin_cases", lit(total_postlogin_cases))
    .withColumn("total_distinct_users", lit(total_distinct_users))
    .withColumn(
        "rep_rate",
        (col("follow_up_1") + col("follow_up_2") + col("follow_up_3+")) / col("total_case")
    )
)

final_df.show(truncate=False)
