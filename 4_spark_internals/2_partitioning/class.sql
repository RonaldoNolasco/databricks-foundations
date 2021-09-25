-- iniciando clase
%run ./Includes/Classroom-Setup

/*%md
### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Get partitions and cores

Use an `rdd` method to get the number of DataFrame partitions*/
df = spark.read.parquet(eventsPath)
df.rdd.getNumPartitions()

/*%md
Access SparkContext through SparkSession to get the number of cores or slots

SparkContext is also provided in Databricks notebooks as the variable `sc`*/
print(spark.sparkContext.defaultParallelism)
# print(sc.defaultParallelism)

/*%md
#### `repartition`
Returns a new DataFrame that has exactly `n` partitions.*/
repartitionedDF = df.repartition(8)
repartitionedDF.rdd.getNumPartitions()

/*%md
#### `coalesce`
Returns a new DataFrame that has exactly `n` partitions, when the fewer partitions are requested

If a larger number of partitions is requested, it will stay at the current number of partitions*/

coalesceDF = df.coalesce(8)
coalesceDF.rdd.getNumPartitions()

/*%md
### ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Configure default shuffle partitions

Use `SparkConf` to access the spark configuration parameter for default shuffle partitions*/
spark.conf.get("spark.sql.shuffle.partitions")
/*%md
Configure default shuffle partitions to match the number of cores*/
spark.conf.set("spark.sql.shuffle.partitions", "8")

/*%md
###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Adaptive Query Execution

In Spark 3, <a href="https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution" target="_blank">AQE</a> is now able to <a href="https://databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html" target="_blank"> dynamically coalesce shuffle partitions</a> at runtime

Spark SQL can use `spark.sql.adaptive.enabled` to control whether AQE is turned on/off (disabled by default)*/

spark.conf.get("spark.sql.adaptive.enabled")

--cerrando aula
%run ./Includes/Classroom-Cleanup


