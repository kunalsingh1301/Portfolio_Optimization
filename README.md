from pyspark.sql.functions import (
    col, when, regexp_extract, concat, lit, lpad, to_timestamp, coalesce
)

def parse_timestamp(df, ts_col):
    """
    SAFE & FAST timestamp normalizer
    Handles:
    - dd-MM-yy HH:mm:ss
    - dd-MM-yyyy HH:mm:ss
    - dd-MM-yy hh:mm:ss AM/PM
    - dd-MM-yyyy hh:mm:ss AM/PM
    - INVALID: HH + AM/PM (24-hr with AM/PM)
    """

    # detect AM/PM once
    df = df.withColumn(
        "_has_ampm",
        col(ts_col).rlike("(?i)\\s(AM|PM)$")
    )

    # extract hour safely for ALL rows (null if not matched)
    df = df.withColumn(
        "_hour_24",
        regexp_extract(col(ts_col), r"\s(\d{1,2}):", 1).cast("int")
    )

    # normalize ONLY invalid AM/PM rows
    df = df.withColumn(
        "_norm_ts",
        when(
            col("_has_ampm") & (col("_hour_24") > 12),
            concat(
                regexp_extract(col(ts_col), r"^(\d{1,2}-\d{1,2}-\d{2,4})", 1),
                lit(" "),
                lpad((col("_hour_24") - 12).cast("string"), 2, "0"),
                lit(":"),
                regexp_extract(col(ts_col), r":(\d{2}:\d{2}\s(?i)(AM|PM))", 1)
            )
        ).otherwise(col(ts_col))
    )

    # parse once (no multiple passes)
    df = df.withColumn(
        "event_ts",
        coalesce(
            to_timestamp(col("_norm_ts"), "dd-MM-yy hh:mm:ss a"),
            to_timestamp(col("_norm_ts"), "dd-MM-yyyy hh:mm:ss a"),
            to_timestamp(col("_norm_ts"), "dd-MM-yy HH:mm:ss"),
            to_timestamp(col("_norm_ts"), "dd-MM-yyyy HH:mm:ss"),
            to_timestamp(col("_norm_ts"), "dd-MM-yy HH:mm"),
            to_timestamp(col("_norm_ts"), "dd-MM-yyyy HH:mm")
        )
    )

    return df.drop("_has_ampm", "_hour_24", "_norm_ts")
