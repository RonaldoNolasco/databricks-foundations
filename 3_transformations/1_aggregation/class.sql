-- abriendo la clase
%run ./Includes/Classroom-Setup

-- importando la data
df = spark.read.parquet(eventsPath)
display(df)

-- agrupando por event_name
df.groupBy("event_name")

-- agrupando por 2 columnas
df.groupBy("geo.state", "geo.city")

-- agrupando por event_name y contando por cada uno
eventCountsDF = df.groupBy("event_name").count()
display(eventCountsDF)

-- agrupando por state y hallando el promedio de compra
avgStatePurchasesDF = df.groupBy("geo.state").avg("ecommerce.purchase_revenue_in_usd")
display(avgStatePurchasesDF)

-- agrupando por state y city y sumando los items totales
cityPurchaseQuantitiesDF = df.groupBy("geo.state", "geo.city").sum("ecommerce.total_item_quantity")
display(cityPurchaseQuantitiesDF)

-- usando funciones de agregacion (alias)
from pyspark.sql.functions import sum

statePurchasesDF = df.groupBy("geo.state").agg(sum("ecommerce.total_item_quantity").alias("total_purchases"))
display(statePurchasesDF)

-- usando varias funciones de agregacion al mismo tiempo
from pyspark.sql.functions import avg, approx_count_distinct

stateAggregatesDF = df.groupBy("geo.state").agg(
  avg("ecommerce.total_item_quantity").alias("avg_quantity"),
  approx_count_distinct("user_id").alias("distinct_users"))

display(stateAggregatesDF)
