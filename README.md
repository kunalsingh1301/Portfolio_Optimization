from pyspark.sql.functions import coalesce, to_timestamp, regexp_replace, date_format, col

def normalize_timestamp(df, ts_col):
    """
    Parse messy datetime strings to dd-MM-yyyy HH:mm:ss
    Handles:
      - 2 or 4 digit years
      - Missing seconds
      - 12h or 24h with AM/PM
    """
    # Step 1: Normalize AM/PM for 24-hour time (remove invalid AM/PM)
    df = df.withColumn("_ts_clean", regexp_replace(col(ts_col), r"([1-2]?[0-9]):([0-5][0-9])\s?(AM|PM)$", r"\1:\2"))

    # Step 2: Fill missing seconds
    df = df.withColumn("_ts_clean",
                       when(col("_ts_clean").rlike(r"^\d{1,2}-\d{1,2}-\d{2,4} \d{1,2}:\d{2}$"),
                            concat_ws(":", col("_ts_clean"), lit("00")))
                       .otherwise(col("_ts_clean")))

    # Step 3: Try multiple formats
    formats = [
        "d-M-yy HH:mm:ss",
        "d-M-yy hh:mm:ss a",
        "d-M-yy HH:mm",
        "d-M-yy hh:mm a",
        "d-M-yyyy HH:mm:ss",
        "d-M-yyyy hh:mm:ss a",
        "d-M-yyyy HH:mm",
        "d-M-yyyy hh:mm a"
    ]
    exprs = [to_timestamp(col("_ts_clean"), f) for f in formats]
    df = df.withColumn("event_ts", coalesce(*exprs))

    # Step 4: Standardize format
    df = df.withColumn("event_ts", date_format(col("event_ts"), "dd-MM-yyyy HH:mm:ss"))
    return df.drop("_ts_clean")
