window = Window.orderBy("user_id", "channel")
rows_per_file = 1000000

agg_df = agg_df.withColumn("rn", row_number().over(window))
agg_df = agg_df.withColumn("batch", ((col("rn")-1)/rows_per_file).cast("int"))

OUTPUT_DIR = "/user/2030435/CallCentreAnalystics/CombinedCallChatStacyNature"

batches = [r["batch"] for r in agg_df.select("batch").distinct().collect()]
for b in batches:
    batch_df = agg_df.filter(col("batch") == b).drop("rn","batch")
    batch_dir = f"{OUTPUT_DIR}/batch_{b}"
    batch_df.coalesce(1).write.mode("overwrite").option("header", True).csv(batch_dir)

    # rename part file
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(batch_dir)
    for f in fs.listStatus(path):
        name = f.getPath().getName()
        if name.startswith("part-") and name.endswith(".csv"):
            fs.rename(f.getPath(), spark._jvm.org.apache.hadoop.fs.Path(batch_dir + f"/CombinedCallChatStacyNature_batch{b}.csv"))

print(f"Finished writing {len(batches)} batches (~1M rows each)!")
✅ What this pipeline doe
