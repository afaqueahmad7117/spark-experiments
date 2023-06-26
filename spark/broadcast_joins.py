import time

from pyspark.sql.types import *
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    # Approach 1
    df_huge = spark.range(1, 10000000)

    data = [
        (1, "mocha"),
        (1, "chai"),
        (1, "latte"),
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ]
    )
    df_small = spark.createDataFrame(data=data, schema=schema)

    df_joined = df_huge.join(df_small, on="id", how="inner")
    df_joined.explain()
    start_time = time.time()
    df_joined.show()
    print(f"Time taken: {time.time() - start_time} seconds")

    # Approach 2

    import pyspark.sql.functions as F

    df_broadcast_joined = df_huge.join(F.broadcast(df_small), on="id", how="inner")
    df_broadcast_joined.explain()
    start_time = time.time()
    df_broadcast_joined.show()
    print(f"Time taken: {time.time() - start_time} seconds")
