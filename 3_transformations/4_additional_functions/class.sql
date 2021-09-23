-- abriendo la clase
%run ./Includes/Classroom-Setup

-- importando datasets
# sale transactions at BedBricks
salesDF = spark.read.parquet(salesPath)
display(salesDF)

# user IDs and emails at BedBricks
usersDF = spark.read.parquet(usersPath)
display(usersDF)

# events logged on the BedBricks website
eventsDF = spark.read.parquet(eventsPath)
display(eventsDF)

/*%md
### 1. Get emails of converted users from transactions
- Select **`email`** column in **`salesDF`** and remove duplicates
- Add new column **`converted`** with value **`True`** for all rows

Save result as **`convertedUsersDF`**.*/

from pyspark.sql.functions import col, lit
convertedUsersDF = (salesDF.select("email")
                    .distinct()
                    .withColumn("converted", lit(True))
)
display(convertedUsersDF)

/*%md
### 2. Join emails with user IDs
- Perform an outer join on **`convertedUsersDF`** and **`usersDF`** with the **`email`** field
- Filter for users where **`email`** is not null
- Fill null values in **`converted`** as **`False`**

Save result as **`conversionsDF`**.*/

conversionsDF = (usersDF.join(convertedUsersDF, "email", "outer")
                 .filter(col("email").isNotNull())
                 .na.fill(False)
)
display(conversionsDF)
/*mal
conversionsDF = (usersDF.join(convertedUsersDF, usersDF["email"] == convertedUsersDF["email"], "outer")
                 .filter(usersDF["email"].isNotNull() & convertedUsersDF["email"].isNotNull())
                 .na.fill(False)
)
display(conversionsDF.count())
210370*/

/*%md
### 3. Get cart item history for each user
- Explode **`items`** field in **`eventsDF`**
- Group by **`user_id`**
  - Collect set of all **`items.item_id`** objects for each user and alias with "cart"

Save result as **`cartsDF`**.*/

from pyspark.sql.functions import *
cartsDF = (eventsDF.withColumn("items", explode("items"))
           .groupBy("user_id")
           .agg(collect_set("items.item_id").alias("cart"))
           
)
display(cartsDF)

/*%md
### 4. Join cart item history with emails
- Perform a left join on **`conversionsDF`** and **`cartsDF`** on the **`user_id`** field

Save result as **`emailCartsDF`**.*/

emailCartsDF = conversionsDF.join(cartsDF, "user_id", "left")
display(emailCartsDF)
/*mal
emailCartsDF = conversionsDF.join(cartsDF, conversionsDF["user_id"] == cartsDF["user_id"])
display(emailCartsDF)
782749*/

/*%md
### 5. Filter for emails with abandoned cart items
- Filter **`emailCartsDF`** for users where **`converted`** is False
- Filter for users with non-null carts

Save result as **`abandonedItemsDF`**.*/

abandonedCartsDF = (emailCartsDF.filter((col("converted") == False) & (col("cart").isNotNull()))
)
display(abandonedCartsDF)

/*%md
### Bonus: Plot number of abandoned cart items by product*/

abandonedItemsDF = (abandonedCartsDF.withColumn("items", explode("cart"))
  .groupBy("items").count()
  .sort("items")
)
display(abandonedItemsDF)

-- cerrando la clase
%run ./Includes/Classroom-Cleanup
