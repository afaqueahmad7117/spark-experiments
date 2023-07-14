# Spark Experiments

<h1 align="center">
    <img
      align="center"
      src="data/spark-experiments.gif"
      style="width:100%;"
    />
</h1>

<p align="center">
  🧙 My experiments with Spark, understanding it's workings under the hood better!
</p>

Refer to datasets used in codes [here.](data)

1. Reading Spark's [query plans](spark/reading_query_plans.ipynb)
2. Data Skew
   1. Generating a [skewed dataset](spark/data_skew/1_generate_skewed_data.ipynb)
   2. Simulating how a [skewed dataset looks like](spark/data_skew/2_skew_dataset_simulation.ipynb)
   3. Solving data skew using [AQE and broadcast joins](spark/data_skew/3_solving_data_skew_aqe_broadcast.ipynb)
   4. Solving data skew (in joins and aggregations) using [salting](spark/data_skew/4_salting.ipynb)
