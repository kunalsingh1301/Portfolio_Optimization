combined_df = combined_df.withColumn(
    "_row_id", monotonically_increasing_id()
)

w = Window.partitionBy("user_id").orderBy("_row_id")

starter_df = (
    combined_df
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .select(
        col("user_id"),
        col("channel").alias("starter_channel")
    )
)

# --------------------------------------------------
# COUNT STARTER CUSTOMERS PER CHANNEL
# --------------------------------------------------
starter_cnt_df = (
    starter_df
    .groupBy("starter_channel")
    .agg(countDistinct("user_id").alias("starter_customers"))
    .withColumnRenamed("starter_channel", "Channel")
)
