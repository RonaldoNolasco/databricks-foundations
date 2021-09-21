-- leyendo el parquet
eventsDF = spark.read.parquet(eventsPath)
display(eventsDF)

-- creando una nueva columna en base al revenue
revenueDF = eventsDF.withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))
display(revenueDF)

-- verificando
expected1 = [5830.0, 5485.0, 5289.0, 5219.1, 5180.0, 5175.0, 5125.0, 5030.0, 4985.0, 4985.0]
result1 = [row.revenue for row in revenueDF.sort(col("revenue").desc_nulls_last()).limit(10).collect()]

assert(expected1 == result1)

-- filtrando cuando el revenue es nulo
purchasesDF = revenueDF.filter(col("revenue").isNotNull())
display(purchasesDF)

-- filtrando los nombres de evento duplicados
distinctDF = purchasesDF.dropDuplicates(["event_name"])
display(distinctDF)

-- eliminando la columna de event_name
finalDF = purchasesDF.drop(col("event_name"))
display(finalDF)

-- realizando todos los pasos menos el de borrar duplicados
# TODO
finalDF = (eventsDF
   .withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))
   .filter(col("revenue").isNotNull())
   .drop(col("event_name"))
)

display(finalDF)

-- verificacion 1
assert(finalDF.count() == 180678)

-- verificacion 2
expected_columns = {'device', 'ecommerce', 'event_previous_timestamp', 'event_timestamp',
                    'geo', 'items', 'revenue', 'traffic_source',
                    'user_first_touch_timestamp', 'user_id'}
assert(set(finalDF.columns) == expected_columns)

-- cerrando aula
%run ./Includes/Classroom-Cleanup
