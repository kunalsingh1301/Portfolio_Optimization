from pyspark.sql.functions import (
    col, trim, regexp_replace, when,
    to_timestamp, coalesce,
    unix_timestamp, from_unixtime, lit
)

def normalize_timestamp(df, ts_col):

    # -------------------------------------------------
    # 1. Raw string
    # -------------------------------------------------
    df = df.withColumn("_raw_ts", trim(col(ts_col)))

    # -------------------------------------------------
    # 2. Normalize Excel numeric strings
    #    "45926 0.7653" -> "45926.7653"
    # -------------------------------------------------
    df = df.withColumn(
        "_excel_str",
        when(
            col("_raw_ts").rlike(r"^\d+\s+\d+(\.\d+)?$"),
            regexp_replace(col("_raw_ts"), r"\s+", ".")
        ).when(
            col("_raw_ts").rlike(r"^\d+(\.\d+)?$"),
            col("_raw_ts")
        )
    )

    # -------------------------------------------------
    # 3. Convert Excel string -> DOUBLE
    # -------------------------------------------------
    df = df.withColumn(
        "_excel_double",
        col("_excel_str").cast("double")
    )

    # -------------------------------------------------
    # 4. Excel DOUBLE -> timestamp
    # -------------------------------------------------
    df = df.withColumn(
        "_excel_ts",
        when(
            col("_excel_double").isNotNull(),
            from_unixtime(
                unix_timestamp(lit("1899-12-30")) +
                (col("_excel_double") * 86400)
            )
        )
    )

    # -------------------------------------------------
    # 5. String timestamp parsing
    # -------------------------------------------------
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

    df = df.withColumn(
        "_string_ts",
        coalesce(*[to_timestamp(col("_raw_ts"), f) for f in formats])
    )

    # -------------------------------------------------
    # 6. FINAL event_ts
    # -------------------------------------------------
    df = df.withColumn(
        "event_ts",
        when(col("_excel_ts").isNotNull(), col("_excel_ts"))
        .otherwise(col("_string_ts"))
    )

    return df.drop(
        "_raw_ts", "_excel_str", "_excel_double",
        "_excel_ts", "_string_ts"
    )
