-- leyendo archivo con head
singleProductCsvFilePath = "/mnt/training/ecommerce/products/products.csv/part-00000-tid-1663954264736839188-daf30e86-5967-4173-b9ae-d1481d3506db-2367-1-c000.csv"
dbutils.fs.head(singleProductCsvFilePath)

-- leyendo csv en df (inferSchema)
productsCsvPath = "/mnt/training/ecommerce/products/products.csv"
productsDF = (spark.read
  .option("header", True)
  .option("inferSchema", True)
  .csv(productsCsvPath))

productsDF.printSchema()

-- comprobación
assert(productsDF.count() == 12)

-- leyendo con userDefinedSchema
userDefinedSchema = StructType([
  StructField("item_id", StringType(), True),
  StructField("name", StringType(), True),
  StructField("price", DoubleType(), True)
])

productsDF2 = (spark.read
  .option("header", True)
  .schema(userDefinedSchema)
  .csv(productsCsvPath))

display(productsDF2)

-- comprobacion de los header
assert(userDefinedSchema.fieldNames() == ["item_id", "name", "price"])

-- comprobacion de la data
from pyspark.sql import Row

expected1 = Row(item_id="M_STAN_Q", name="Standard Queen Mattress", price=1045.0)
result1 = productsDF2.first()

assert(expected1 == result1)

-- leyendo con DLLSchema
DDLSchema = "item_id string, name string, price double"

productsDF3 = (spark.read
  .option("header", True)
  .schema(DDLSchema)
  .csv(productsCsvPath))

-- comprobacion
assert(productsDF3.count() == 12)

-- escribiendo la data en un archivo
productsOutputPath = workingDir + "/delta/products"
(productsDF.write
  .format("delta")
  .mode("overwrite")
  .save(productsOutputPath)
)

--comprobacion
assert(len(dbutils.fs.ls(productsOutputPath)) == 5)
