spark.sql(f"USE {TARGET_DB}")

# dynamic partitions
spark.sql("SET hive.exec.dynamic.partition=true")
spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

# OPTIONAL: if you rerun same month and want no duplicates, drop partition first (uncomment)
"""
ym_list = [r["yearmonth"] for r in call_df.select("yearmonth").distinct().collect()]
for ym in ym_list:
    spark.sql(f"ALTER TABLE {FULL_TABLE} DROP IF EXISTS PARTITION (yearmonth='{ym}')")
"""

if not spark.catalog.tableExists(FULL_TABLE):
    # Create table (stored as parquet) under this DB (needs table-create permission)
    (call_df
        .repartition(col("yearmonth"))
        .write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("yearmonth")
        .saveAsTable(FULL_TABLE)
    )
    print("Created table:", FULL_TABLE)
else:
    # Append new partitions / rows
    (call_df
        .repartition(col("yearmonth"))
        .write
        .format("parquet")
        .mode("append")
        .insertInto(FULL_TABLE)
    )
    print("Appended to table:", FULL_TABLE)

# Validate
spark.sql(f"SHOW PARTITIONS {FULL_TABLE}").show(200, truncate=False)
spark.sql(f"""
  SELECT yearmonth, COUNT(*) rows_cnt
  FROM {FULL_TABLE}
  GROUP BY yearmonth
  ORDER BY yearmonth
""").show(200, truncate=False)
