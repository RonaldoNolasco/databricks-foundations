-- abriendo clase
%run ./Includes/Classroom-Setup

-- leyendo el df
df = spark.read.parquet(salesPath)
display(df)

/*%md
### 1. Extract item details from purchases
- Explode **`items`** field in **`df`**
- Select **`email`** and **`item.item_name`** fields
- Split words in **`item_name`** into an array and alias with "details"

Assign the resulting DataFrame to **`detailsDF`**.*/
from pyspark.sql.functions import *

detailsDF = (df.withColumn("items", explode("items"))
  .select("email", "items.item_name")
  .withColumn("details", split(col("item_name"), " "))
)
display(detailsDF)

/*%md
### 2. Extract size and quality options from mattress purchases
- Filter **`detailsDF`** for records where **`details`** contains "Mattress"
- Add **`size`** column from extracting element at position 2
- Add **`quality`** column from extracting element at position 1

Save result as **`mattressDF`**.*/
mattressDF = (detailsDF.filter(array_contains(col("details"), "Mattress"))
  .withColumn("size", element_at(col("details"), 2))
  .withColumn("quality", element_at(col("details"), 1))
)
display(mattressDF)

/*%md
### 3. Extract size and quality options from pillow purchases
- Filter **`detailsDF`** for records where **`details`** contains "Pillow"
- Add **`size`** column from extracting element at position 1
- Add **`quality`** column from extracting element at position 2

Note the positions of **`size`** and **`quality`** are switched for mattresses and pillows.

Save result as **`pillowDF`**.*/
pillowDF = (detailsDF.filter(array_contains(col("details"), "Pillow"))
  .withColumn("size", element_at(col("details"), 1))
  .withColumn("quality", element_at(col("details"), 2))
)
display(pillowDF)

/*%md
### 4. Combine data for mattress and pillows
- Perform a union on **`mattressDF`** and **`pillowDF`** by column names
- Drop **`details`** column

Save result as **`unionDF`**.*/
unionDF = (mattressDF.unionByName(pillowDF)
  .drop("details"))
display(unionDF)

/*%md
### 5. List all size and quality options bought by each user
- Group rows in **`unionDF`** by **`email`**
  - Collect set of all items in **`size`** for each user with alias "size options"
  - Collect set of all items in **`quality`** for each user with alias "quality options"

Save result as **`optionsDF`**.*/
optionsDF = (unionDF.groupBy("email")
  .agg(collect_set("size").alias("size options"),
       collect_set("quality").alias("quality options"))
)
display(optionsDF)

-- cerrando aula
%run ./Includes/Classroom-Cleanup

--.filter(usersDF["email"].isNotNull())