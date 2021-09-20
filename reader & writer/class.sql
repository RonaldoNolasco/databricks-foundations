-- abriendo el aula
%run ./Includes/Classroom-Setup

-- Leyendo datos desde csv (inferschema)
usersCsvPath = "/mnt/training/ecommerce/users/users-500k.csv"

usersDF = (spark.read
  .option("sep", "\t")
  .option("header", True)
  .option("inferSchema", True)
  .csv(usersCsvPath))

usersDF.printSchema()

-- Creando una userDefinedSchema en spark
from pyspark.sql.types import LongType, StringType, StructType, StructField

userDefinedSchema = StructType([
  StructField("user_id", StringType(), True),
  StructField("user_first_touch_timestamp", LongType(), True),
  StructField("email", StringType(), True)
])

-- Leyendo datos desde csv (userDefinedSchema)
usersDF = (spark.read
  .option("sep", "\t")
  .option("header", True)
  .schema(userDefinedSchema)
  .csv(usersCsvPath))

-- definiendo un DDLschema para poder depositar la data
DDLSchema = "user_id string, user_first_touch_timestamp long, email string"

-- Leyendo datos desde csv (DDLschema)
usersDF = (spark.read
  .option("sep", "\t")
  .option("header", True)
  .schema(DDLSchema)
  .csv(usersCsvPath))

-- leyendo archivos desde json (inferschema)
eventsJsonPath = "/mnt/training/ecommerce/events/events-500k.json"

eventsDF = (spark.read
  .option("inferSchema", True)
  .json(eventsJsonPath))

eventsDF.printSchema()

-- Creando un struct para leer la data
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, LongType, StringType, StructType, StructField

userDefinedSchema = StructType([
  StructField("device", StringType(), True),
  StructField("ecommerce", StructType([
    StructField("purchaseRevenue", DoubleType(), True),
    StructField("total_item_quantity", LongType(), True),
    StructField("unique_items", LongType(), True)
  ]), True),
  StructField("event_name", StringType(), True),
  StructField("event_previous_timestamp", LongType(), True),
  StructField("event_timestamp", LongType(), True),
  StructField("geo", StructType([
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
  ]), True),
  StructField("items", ArrayType(
    StructType([
      StructField("coupon", StringType(), True),
      StructField("item_id", StringType(), True),
      StructField("item_name", StringType(), True),
      StructField("item_revenue_in_usd", DoubleType(), True),
      StructField("price_in_usd", DoubleType(), True),
      StructField("quantity", LongType(), True)
    ])
  ), True),
  StructField("traffic_source", StringType(), True),
  StructField("user_first_touch_timestamp", LongType(), True),
  StructField("user_id", StringType(), True)
])

-- -- leyendo archivos desde json (struct)
eventsDF = (spark.read
  .schema(userDefinedSchema)
  .json(eventsJsonPath))

-- Obteniendo el DDLschema de un archivo
%scala
spark.read.parquet("/mnt/training/ecommerce/events/events.parquet").schema.toDDL

-- Se copia el resultado del query anterior
DDLSchema = "`device` STRING,`ecommerce` STRUCT<`purchase_revenue_in_usd`: DOUBLE, `total_item_quantity`: BIGINT, `unique_items`: BIGINT>,`event_name` STRING,`event_previous_timestamp` BIGINT,`event_timestamp` BIGINT,`geo` STRUCT<`city`: STRING, `state`: STRING>,`items` ARRAY<STRUCT<`coupon`: STRING, `item_id`: STRING, `item_name`: STRING, `item_revenue_in_usd`: DOUBLE, `price_in_usd`: DOUBLE, `quantity`: BIGINT>>,`traffic_source` STRING,`user_first_touch_timestamp` BIGINT,`user_id` STRING"

-- Y se usa el DDLschema para la importación
eventsDF = (spark.read
  .schema(DDLSchema)
  .json(eventsJsonPath))

-- Escribiendo dataframes a archivos
usersOutputPath = workingDir + "/users.parquet"

(usersDF.write
  .option("compression", "snappy")
  .mode("overwrite")
  .parquet(usersOutputPath)
)

-- Escribiendo dataframes en tablas
eventsDF.write.mode("overwrite").saveAsTable("events_p")
print(databaseName)

-- La mejor practica, escribir df en delta tables
eventsOutputPath = workingDir + "/delta/events"

(eventsDF.write
  .format("delta")
  .mode("overwrite")
  .save(eventsOutputPath)
)
