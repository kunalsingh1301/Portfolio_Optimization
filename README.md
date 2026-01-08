from pyspark.sql.functions import (
    col, trim, when, regexp_replace,
    to_timestamp, coalesce,
    unix_timestamp, from_unixtime, lit
)

def normalize_timestamp(df, ts_col):

    # -------------------------------
    # Raw trimmed string
    # -------------------------------
    df = df.withColumn("_raw_ts", trim(col(ts_col)))

    # -------------------------------
    # Fix Excel split numeric: 45926 0.7653 → 45926.7653
    # -------------------------------
    df = df.withColumn(
        "_excel_num",
        when(
            col("_raw_ts").rlike(r"^\d+\s+\d+(\.\d+)?$"),
            regexp_replace(col("_raw_ts"), r"\s+", ".")
        ).when(
            col("_raw_ts").rlike(r"^\d+(\.\d+)?$"),
            col("_raw_ts")
        )
    )

    # -------------------------------
    # Excel numeric → timestamp
    # -------------------------------
    excel_ts = from_unixtime(
        unix_timestamp(lit("1899-12-30")) +
        (col("_excel_num").cast("double") * 86400)
    )

    # -------------------------------
    # String formats
    # -------------------------------
    formats = [
        "d-M-yy HH:mm:ss", "d-M-yy hh:mm:ss a",
        "d-M-yyyy HH:mm:ss", "d-M-yyyy hh:mm:ss a",
        "d-M-yy HH:mm", "d-M-yy hh:mm a",
        "d-M-yyyy HH:mm", "d-M-yyyy hh:mm a",

        "M-d-yy HH:mm:ss", "M-d-yyyy HH:mm:ss",
        "M-d-yy HH:mm", "M-d-yyyy HH:mm",
        "M-d-yy HH:mm:ss a", "M-d-yyyy HH:mm:ss a",
        "M-d-yy HH:mm a", "M-d-yyyy HH:mm a",

        "dd/MM/yyyy HH:mm"
    ]

    string_ts = coalesce(
        *[to_timestamp(col("_raw_ts"), f) for f in formats]
    )

    # -------------------------------
    # FINAL event_ts (NO MIXING)
    # -------------------------------
    df = df.withColumn(
        "event_ts",
        when(col("_excel_num").isNotNull(), excel_ts)
        .otherwise(string_ts)
    )

    return df.drop("_raw_ts", "_excel_num")
