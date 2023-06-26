from pyspark.sql import SparkSession
import pyspark.sql.functions as F


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Spark Partitioning")
        .enableHiveSupport()
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    df_table_1 = spark.range(1, 1000000000)
    df_table_2 = spark.range(1, 1000000)
    df_table_1 = df_table_1.withColumn("test", F.lit(1))
    df_table_2 = df_table_2.withColumn("test", F.lit(1))

    df_table_1.repartition(5, "id").write.mode("overwrite").partitionBy("id").parquet(
        "data/table_1.parquet"
    )
    df_table_2.repartition(5, "id").write.mode("overwrite").partitionBy("id").parquet(
        "data/table_2.parquet"
    )

    print("done repartitioning")
    df_joined = df_table_1.join(df_table_2, "id")
    df_joined.explain()
    print("done joining")

    df_rep_table_1 = spark.read.parquet("data/table_1.parquet")
    df_rep_table_2 = spark.read.parquet("data/table_2.parquet")
    df_joined_2 = df_rep_table_1.join(df_rep_table_2, "id")
    print("done joining 2")
    df_joined_2.explain()

    # to keep holding the spark ui
    # input()
