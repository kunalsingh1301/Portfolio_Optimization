df = df.withColumn(
    "_ts",
    when(
        col("_ts").rlike(r"^\d+\s+\d+(\.\d+)?$"),  # only matches numeric with space
        regexp_replace(col("_ts"), r"\s+", ".")    # replace space(s) with dot
    ).otherwise(col("_ts"))                        # leave everything else intact
)
