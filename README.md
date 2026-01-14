from pyspark.sql.types import ArrayType, StringType

final_df = final_df \
    .withColumn("primary_nature", concat_ws("|", when(col("primary_nature").isNotNull(), col("primary_nature")).otherwise(array().cast(ArrayType(StringType()))))) \
    .withColumn("secondary_nature", concat_ws("|", when(col("secondary_nature").isNotNull(), col("secondary_nature")).otherwise(array().cast(ArrayType(StringType()))))) \
    .withColumn("chat_nature", concat_ws("|", when(col("chat_nature").isNotNull(), col("chat_nature")).otherwise(array().cast(ArrayType(StringType())))))
