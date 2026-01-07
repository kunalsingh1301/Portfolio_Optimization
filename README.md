df = df.withColumn(
        "_ts",
        when(
            col("_ts").rlike(r"^\d+\s+\d+(\.\d+)?$"),
            regexp_replace(col("_ts"), r"\s+", ".")
        ).otherwise(col("_ts"))
    )
    
    # ---------------- Step 3: convert Excel numeric dates to timestamp ----------------
    df = df.withColumn(
        "_ts",
        when(
            col("_ts").rlike(r"^\d+(\.\d+)?$"),
            expr(
                "timestampadd(SECOND, cast(cast(_ts as double) * 86400 as int), timestamp('1899-12-30'))"
            )
        ).otherwise(col("_ts"))
    )
