-- abriendo clase
%run ./Includes/Classroom-Setup

-- importando dataset
salesDF = spark.read.parquet(salesPath)
display(salesDF)

-- definiendo una funcion en python
def firstLetterFunction(email):
  return email[0]

firstLetterFunction("annagray@kaufman.com")

/*Create and apply UDF
Define a UDF that wraps the function. This serializes the function and sends it to executors to be able to use in our DataFrame.*/
from pyspark.sql.functions import udf
firstLetterUDF = udf(firstLetterFunction)

/*%md
Apply UDF on the `email` column.*/
from pyspark.sql.functions import col
display(salesDF.select(firstLetterUDF(col("email"))))

/*Register UDF to use in SQL
Register UDF using `spark.udf.register` to create UDF in the SQL namespace.*/
salesDF.createOrReplaceTempView("sales")

spark.udf.register("sql_udf", firstLetterFunction)

%sql
SELECT sql_udf(email) AS firstLetter FROM sales

/*Use Decorator Syntax (Python Only)
Alternatively, define UDF using decorator syntax in Python with the datatype the function returns.
You will no longer be able to call the local Python function (e.g. `decoratorUDF("annagray@kaufman.com")` will not work)*/
%python
# Our input/output is a string
@udf("string")
def decoratorUDF(email: str) -> str:
  return email[0]

%python
from pyspark.sql.functions import col
salesDF = spark.read.parquet("/mnt/training/ecommerce/sales/sales.parquet")
display(salesDF.select(decoratorUDF(col("email"))))

/*Use Vectorized UDF (Python Only)
Use Vectorized UDF to help speed up the computation using Apache Arrow.*/
%python
import pandas as pd
from pyspark.sql.functions import pandas_udf

# We have a string input/output
@pandas_udf("string")
def vectorizedUDF(email: pd.Series) -> pd.Series:
  return email.str[0]

# Alternatively
vectorizedUDF = pandas_udf(lambda s: s.str[0], "string")

%python
display(salesDF.select(vectorizedUDF(col("email"))))

/*%md
We can also register these Vectorized UDFs to the SQL namespace.*/
%python
spark.udf.register("sql_vectorized_udf", vectorizedUDF)
