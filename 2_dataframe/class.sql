dataframe: tabla
schema: datos de la tabla
transformation: where, order by
actions: count, collect, show

-- abriendo el aula
%run ./Includes/Classroom-Setup-SQL

-- creando un dataframe (con sentencia SQL)
budgetDF = spark.sql("""
SELECT name, price
FROM products
WHERE price < 200
ORDER BY price
""")

-- mostrando df (show)
budgetDF.show()

-- mostrando df (display)
display(budgetDF)

-- creando un dataframe (con la sparkSession)
productsDF = spark.table("products")
display(productsDF)

-- mostrando el esquema del df (con una función)
productsDF.printSchema()

-- mostrando el esquema del df (con un atributo)
productsDF.schema

-- creando un df (spark con transformaciones)
budgetDF = (productsDF
  .select("name", "price")
  .where("price < 200")
  .orderBy("price")
)

-- contando las filas del df
budgetDF.count()

-- tomando las n filas superiores del df
budgetDF.take(2)

-- creando una vista temporal
budgetDF.createOrReplaceTempView("budget")

-- usando sql en la vista temporal
display(spark.sql("SELECT * FROM budget"))

