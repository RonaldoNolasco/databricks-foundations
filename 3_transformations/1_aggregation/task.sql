-- importando data
from pyspark.sql.functions import col

# purchase events logged on the BedBricks website
df = (spark.read.parquet(eventsPath)
  .withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))
  .filter(col("revenue").isNotNull())
  .drop("event_name"))

display(df)

/*%md
### 1. Aggregate revenue by traffic source
- Group by **`traffic_source`**
- Get sum of **`revenue`** as **`total_rev`**
- Get average of **`revenue`** as **`avg_rev`**

Remember to import any necessary built-in functions.*/
trafficDF = (df.groupBy("traffic_source")
             .agg(sum("revenue").alias("total_rev"),
             avg("revenue").alias("avg_rev"))
)

display(trafficDF)

-- verificacion
from pyspark.sql.functions import round
expected1 = [(12704560.0, 1083.175), (78800000.3, 983.2915), (24797837.0, 1076.6221), (47218429.0, 1086.8303), (16177893.0, 1083.4378), (8044326.0, 1087.218)]
testDF = trafficDF.sort("traffic_source").select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result1 = [(row.total_rev, row.avg_rev) for row in testDF.collect()]

assert(expected1 == result1)

/*%md
### 2. Get top three traffic sources by total revenue
- Sort by **`total_rev`** in descending order
- Limit to first three rows*/
topTrafficDF = (trafficDF.sort(col("total_rev").desc()).limit(3)
)
display(topTrafficDF)

-- verificacion
expected2 = [(78800000.3, 983.2915), (47218429.0, 1086.8303), (24797837.0, 1076.6221)]
testDF = topTrafficDF.select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result2 = [(row.total_rev, row.avg_rev) for row in testDF.collect()]

assert(expected2 == result2)

/*%md
### 3. Limit revenue columns to two decimal places
- Modify columns **`avg_rev`** and **`total_rev`** to contain numbers with two decimal places
  - Use **`withColumn()`** with the same names to replace these columns
  - To limit to two decimal places, multiply each column by 100, cast to long, and then divide by 100*/
-- dato (una columna puede usarse para modificarse a si misma)
finalDF = (topTrafficDF
   .withColumn("avg_rev", (col("avg_rev") * 100).cast("long")/100)
   .withColumn("total_rev", (col("total_rev") * 100).cast("long")/100)
)

display(finalDF)

-- verificacion
expected3 = [(78800000.29, 983.29), (47218429.0, 1086.83), (24797837.0, 1076.62)]
result3 = [(row.total_rev, row.avg_rev) for row in finalDF.collect()]

assert(expected3 == result3)

/*%md
### 4. Bonus: Rewrite using a built-in math function
Find a built-in math function that rounds to a specified number of decimal places*/
bonusDF = (topTrafficDF
   .withColumn("avg_rev", round(col("avg_rev"),2))
   .withColumn("total_rev", round(col("total_rev"),2))
)

display(bonusDF)

-- verificacion
expected4 = [(78800000.3, 983.29), (47218429.0, 1086.83), (24797837.0, 1076.62)]
result4 = [(row.total_rev, row.avg_rev) for row in bonusDF.collect()]

assert(expected4 == result4)

/*%md
### 5. Chain all the steps above*/
chainDF = (df.groupBy("traffic_source")
   .agg(sum("revenue").alias("total_rev"),
    avg("revenue").alias("avg_rev"))
   .sort(col("total_rev").desc()).limit(3)
   .withColumn("avg_rev", round(col("avg_rev"),2))
   .withColumn("total_rev", round(col("total_rev"),2))
)

display(chainDF)

-- verificacion
expected5 = [(78800000.3, 983.29), (47218429.0, 1086.83), (24797837.0, 1076.62)]
result5 = [(row.total_rev, row.avg_rev) for row in chainDF.collect()]

assert(expected5 == result5)

-- cerrando el aula
%run ./Includes/Classroom-Cleanup
