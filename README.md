from pyspark.sql.functions import (
    col, when, regexp_extract, concat, lpad, lit
)

def normalize_bad_ampm(df, ts_col):
    """
    Fix invalid timestamps like:
    dd-MM-yy HH:mm:ss AM/PM
    where HH > 12 and AM/PM exists
    """

    # Extract components
    df = df.withColumn("day_part", regexp_extract(col(ts_col), r"^(\d{1,2}-\d{1,2}-\d{2,4})", 1))
    df = df.withColumn("hour_24", regexp_extract(col(ts_col), r"\s(\d{1,2}):", 1).cast("int"))
    df = df.withColumn("rest_part", regexp_extract(col(ts_col), r"\s\d{1,2}:(\d{2}:\d{2}\s(?:AM|PM))", 1))

    # Build normalized timestamp
    df = df.withColumn(
        "_norm_ts",
        when(
            (col(ts_col).rlike("AM|PM")) & (col("hour_24") > 12),
            concat(
                col("day_part"),
                lit(" "),
                lpad((col("hour_24") - 12).cast("string"), 2, "0"),
                lit(":"),
                col("rest_part")
            )
        ).otherwise(col(ts_col))
    )

    return df.drop("day_part", "hour_24", "rest_part")
