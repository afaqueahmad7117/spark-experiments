from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, array
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as f

spark = SparkSession.builder.master("local[5]").getOrCreate()


#################
# NO SKEW
#################

# df_evenly = spark.createDataFrame([i for i in range(1000000)], IntegerType())
# df_evenly = df_evenly.repartition(3)
# df_evenly = df_evenly.withColumn("partitionId", spark_partition_id())
# print(f"number of partitions: {df_evenly.rdd.getNumPartitions()}")
# df_evenly.groupby([df_evenly.partitionId]).count().sort(df_evenly.partitionId).show()
#
# print(df_evenly.alias("left").join(df_evenly.alias("right"), "value", "inner").count())


#################
# SKEW
#################

df0 = spark.createDataFrame([0] * 999990, IntegerType()).repartition(1)
df1 = spark.createDataFrame([1] * 5, IntegerType()).repartition(1)
df2 = spark.createDataFrame([2] * 5, IntegerType()).repartition(1)
df_skew = df0.union(df1).union(df2)
df_skew = df_skew.withColumn("partition_id", spark_partition_id())
df_skew.cache()
df_skew.groupby([df_skew.partition_id]).count().sort(df_skew.partition_id).show()

# simulate reading to first round robin distribute the key
# df_skew = df_skew.repartition(3)
# df_skew.groupby([df_skew.partitionId]).count().sort(df_skew.partitionId).show()
# print(df_skew.join(df_skew.select("value"), "value", "inner").count())


#################
# SKEW CORRECTED
#################

# n = 5
# salt_values = list(range(n))
#
# df_skew_p1 = df_skew.withColumn("salt", f.monotonically_increasing_id() % n)
# df_skew_p1.show(15, False)
# df_skew_p2 = (
#     df_skew
#     .withColumn("salt_values", array([f.lit(i) for i in salt_values]))
#     .withColumn("salt", f.explode(f.col("salt_values")))
# )
# df_skew_p2.show(15, False)
#
# join_condition = [df_skew_p1.value == df_skew_p2.value, df_skew_p1.salt == df_skew_p2.salt]
# df_joined = df_skew_p1.join(df_skew_p2, join_condition, 'inner').drop("salt")
#
# df_joined = df_joined.withColumn("partition", spark_partition_id())
# df_joined.groupby([df_joined.partition]).count().sort(df_joined.partition).show()

input()
spark.stop()
