-- abriendo clase
%run ./Includes/Classroom-Setup

-- leyendo un df (desde un parquet)
eventsDF = spark.read.parquet(eventsPath)
display(eventsDF)

-- verificando la creación de columnas
from pyspark.sql.functions import col

col("device")
eventsDF.device
eventsDF["device"]

-- usando expresiones complejas
col("ecommerce.purchase_revenue_in_usd") + col("ecommerce.total_item_quantity")
col("event_timestamp").desc()
(col("ecommerce.purchase_revenue_in_usd") * 100).cast("int")

-- ejemplo en un dataframe
revDF = (eventsDF.filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
  .withColumn("purchase_revenue", (col("ecommerce.purchase_revenue_in_usd") * 100).cast("int"))
  .withColumn("avg_purchase_revenue", col("ecommerce.purchase_revenue_in_usd") / col("ecommerce.total_item_quantity"))
  .sort(col("avg_purchase_revenue").desc()))

display(revDF)

-- Ahora si
-- obteniendo el user_id y el device de los eventos
devicesDF = eventsDF.select("user_id", "device")
display(devicesDF)

-- obteniendo subcolumnas de las columnas (geo es un objeto)
from pyspark.sql.functions import col

locationsDF = eventsDF.select("user_id",
  col("geo.city").alias("city"),
  col("geo.state").alias("state"))

display(locationsDF)

-- obteniendo el user id, y una nueva columna de validacion de usuario de mac o ios
appleDF = eventsDF.selectExpr("user_id", "device in ('macOS', 'iOS') as apple_user")
display(appleDF)

-- creando un nuevo df quitando columnas del original
-- metodo 1
anonymousDF = eventsDF.drop("user_id", "geo", "device")
display(anonymousDF)

-- metodo 2
noSalesDF = eventsDF.drop(col("ecommerce"))
display(noSalesDF)

-- creando una nueva columna en base a otras
-- verificando si es usuario de movil
mobileDF = eventsDF.withColumn("mobile", col("device").isin("iOS", "Android"))
display(mobileDF)

-- pasando todas las ventas a enteros
purchaseQuantityDF = eventsDF.withColumn("purchase_quantity", col("ecommerce.total_item_quantity").cast("int"))
purchaseQuantityDF.printSchema()

-- renombrando una columna
locationDF = eventsDF.withColumnRenamed("geo", "location")
display(locationDF)

-- realizando filtros
-- la cantidad de items mayor a cero
purchasesDF = eventsDF.filter("ecommerce.total_item_quantity > 0")
display(purchasesDF)

-- el monto gastado no sea nulo
revenueDF = eventsDF.filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
display(revenueDF)

-- usuarios de android con trafico no directo
androidDF = eventsDF.filter((col("traffic_source") != "direct") & (col("device") == "Android"))
display(androidDF)

-- removiendo las filas duplicadas
eventsDF.distinct()

-- removiendo los duplicados de una columna
distinctUsersDF = eventsDF.dropDuplicates(["user_id"])
display(distinctUsersDF)

-- mostrando las primeras n filas
limitDF = eventsDF.limit(100)
display(limitDF)

-- ordernando por una columna
increaseTimestampsDF = eventsDF.sort("event_timestamp")
display(increaseTimestampsDF)

-- ordernando por una columna en descendiente
decreaseTimestampsDF = eventsDF.sort(col("event_timestamp").desc())
display(decreaseTimestampsDF)

-- ordernando por varias columnas
increaseSessionsDF = eventsDF.orderBy(["user_first_touch_timestamp", "event_timestamp"])
display(increaseSessionsDF)

-- ordernando por varias columnas en sentido mixto
decreaseSessionsDF = eventsDF.sort(col("user_first_touch_timestamp").desc(), col("event_timestamp"))
display(decreaseSessionsDF)
