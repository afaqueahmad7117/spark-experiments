from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession
        .builder
        .appName("Spark Bucketing")
        .enableHiveSupport()
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    df_table_1 = spark.range(1, 1000000000)
    df_table_2 = spark.range(1, 1000000)

    df_join = df_table_2.join(df_table_1, on="id", how="inner")
    df_join.count()
    df_join.explain()

    (
        df_table_1
        .write
        .bucketBy(5, "id")
        .sortBy("id")
        .mode("overwrite")
        .saveAsTable("table_1_bucketed")
    )

    (
        df_table_2
        .write
        .bucketBy(5, "id")
        .sortBy("id")
        .mode("overwrite")
        .saveAsTable("table_2_bucketed")
    )

    df_table_1_bucketed = spark.table("table_1_bucketed")
    df_table_2_bucketed = spark.table("table_2_bucketed")
    df_bucketed_join = df_table_2_bucketed.join(df_table_1_bucketed, on="id", how="inner")
    df_bucketed_join.count()
    df_bucketed_join.explain()

    # to keep holding the spark ui
    input()


if __name__ == "__main__":
    main()
