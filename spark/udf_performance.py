from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.csv("data/customer.csv", header=True, inferSchema=True)
    df.printSchema()
    print(df.count())

    df.write.mode("overwrite").parquet("data/customer.parquet")

    df = spark.read.parquet("data/customer.parquet")
    df.where('name == "afaque"').explain(mode="cost")

    def is_name(name):
        return name == "afaque"

    is_name_udf = udf(lambda name: is_name(name), BooleanType())

    df.where(is_name_udf("name")).show()
    df.where(is_name_udf("name")).explain(mode="cost")
