-- iniciando clase
%run ./Includes/Classroom-Setup

-- leyendo df
df = spark.read.parquet(eventsPath)
display(df)

-- optimizacion logica
from pyspark.sql.functions import col

limitEventsDF = (df
  .filter(col("event_name") != "reviews")
  .filter(col("event_name") != "checkout")
  .filter(col("event_name") != "register")
  .filter(col("event_name") != "email_coupon")
  .filter(col("event_name") != "cc_info")
  .filter(col("event_name") != "delivery")
  .filter(col("event_name") != "shipping_info")
  .filter(col("event_name") != "press")
)

limitEventsDF.count()

/*%md
#### `explain(..)`

Prints the plans (logical and physical), optionally formatted by a given explain mode*/
limitEventsDF.explain(True)

-- optimizacion
betterDF = (df.filter(
  (col("event_name").isNotNull()) &
  (col("event_name") != "reviews") &
  (col("event_name") != "checkout") &
  (col("event_name") != "register") &
  (col("event_name") != "email_coupon") &
  (col("event_name") != "cc_info") &
  (col("event_name") != "delivery") &
  (col("event_name") != "shipping_info") &
  (col("event_name") != "press")
))

betterDF.count()

betterDF.explain(True)

-- otro ejemplo
stupidDF = (df
  .filter(col("event_name") != "finalize")
  .filter(col("event_name") != "finalize")
  .filter(col("event_name") != "finalize")
  .filter(col("event_name") != "finalize")
  .filter(col("event_name") != "finalize")
)

stupidDF.explain(True)

/*%md
/*Predicate Pushdown

Here is example with JDBC where predicate pushdown takes place*/
%scala
// Ensure that the driver class is loaded
Class.forName("org.postgresql.Driver")

jdbcURL = "jdbc:postgresql://54.213.33.240/training"

# Username and Password w/read-only rights
connProperties = {
  "user" : "training",
  "password" : "training"
}

ppDF = (spark.read.jdbc(
    url=jdbcURL,                  # the JDBC URL
    table="training.people_1m",   # the name of the table
    column="id",                  # the name of a column of an integral type that will be used for partitioning
    lowerBound=1,                 # the minimum value of columnName used to decide partition stride
    upperBound=1000000,           # the maximum value of columnName used to decide partition stride
    numPartitions=8,              # the number of partitions/connections
    properties=connProperties     # the connection properties
  )
  .filter(col("gender") == "M")   # Filter the data by gender
)

ppDF.explain()

/*%md
No Predicate Pushdown

This will make a little more sense if we **compare it to examples** that don't push down the filter.*/
/*%md
Caching the data before filtering eliminates the possibility for the predicate push down*/

cachedDF = (spark.read.jdbc(
    url=jdbcURL,
    table="training.people_1m",
    column="id",
    lowerBound=1,
    upperBound=1000000,
    numPartitions=8,
    properties=connProperties
  ))

cachedDF.cache().count()

filteredDF = cachedDF.filter(col("gender") == "M")

/*%md
In addition to the **Scan** (the JDBC read) we saw in the previous example, here we also see the **InMemoryTableScan** followed by a **Filter** in the explain plan.

This means Spark had to filter ALL the data from RAM instead of in the Database.*/
filteredDF.explain()

/*%md
Here is another example using CSV where predicate pushdown does **not** place*/
csvDF = (spark.read
  .option("header", "true")
  .option("sep", "\t")
  .option("inferSchema", "true")
  .csv("/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv")
  .filter(col("site") == "desktop"))

/*%md
Note the presence of a **Filter** and **PushedFilters** in the **FileScan csv**

Again, we see **PushedFilters** because Spark is *trying* to push down to the CSV file.

However, this does not work here, and thus we see, like in the last example, we have a **Filter** after the **FileScan**, actually an **InMemoryFileIndex**.*/

csvDF.explain()

--cerrando aula
%run ./Includes/Classroom-Cleanup
