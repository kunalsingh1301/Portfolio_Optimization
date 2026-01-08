df.select(
        col("_ts").alias("raw_ts"),
        hex(col("_ts")).alias("raw_hex"),
        length(col("_ts")).alias("raw_len"),
        trim(col("_ts")).alias("trimmed"),
        regexp_replace(col("_ts"), r"\s+", " ").alias("ws_normalized"),
        regexp_replace(col("_ts"), r"\s+", ".").alias("space_to_dot"),
        col("_ts").rlike(r"^\d+\s+\d+(\.\d+)?$").alias("is_excel_split"),
        col("_ts").rlike(r"^\d+(\.\d+)?$").alias("is_excel_plain"),
        regexp_replace(col("_ts"), r"\s+", ".").cast("double").alias("as_double")
    ).show(50, False)
