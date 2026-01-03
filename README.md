from pyspark.sql.functions import (
    col, when, regexp_extract, concat, lit, lpad,
    coalesce, try_to_timestamp
)

def parse_timestamp(df, ts_col):

    df = df.withColumn(
        "_has_ampm",
        col(ts_col).rlike("(?i)\\s(AM|PM)$")
    )

    df = df.withColumn(
        "_hour",
        regexp_extract(col(ts_col), r"\s(\d{1,2}):", 1).cast("int")
    )

    # ---- normalize ONLY bad 24h + AM/PM ----
    df = df.withColumn(
        "_norm_ts",
        when(
            col("_has_ampm") & (col("_hour") > 12),
            concat(
                regexp_extract(col(ts_col), r"^(\d{1,2}-\d{1,2}-\d{2,4})", 1),
                lit(" "),
                lpad((col("_hour") - 12).cast("string"), 2, "0"),
                lit(":"),
                regexp_extract(col(ts_col), r":(\d{2}:\d{2}\s(?i)(AM|PM))", 1)
            )
        ).otherwise(col(ts_col))
    )

    # ---- SAFE PARSING (NO INVALID FORMAT EVER TRIED) ----
    df = df.withColumn(
        "event_ts",
        when(
            col("_has_ampm"),
            coalesce(
                try_to_timestamp(col("_norm_ts"), "dd-MM-yy hh:mm:ss a"),
                try_to_timestamp(col("_norm_ts"), "dd-MM-yyyy hh:mm:ss a")
            )
        ).otherwise(
            coalesce(
                try_to_timestamp(col("_norm_ts"), "dd-MM-yy HH:mm:ss"),
                try_to_timestamp(col("_norm_ts"), "dd-MM-yyyy HH:mm:ss"),
                try_to_timestamp(col("_norm_ts"), "dd-MM-yy HH:mm"),
                try_to_timestamp(col("_norm_ts"), "dd-MM-yyyy HH:mm")
            )
        )
    )

    return df.drop("_has_ampm", "_hour", "_norm_ts")
