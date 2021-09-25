/*%md-sandbox
# DataFrames and Transformations Review
##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) De-Duping Data Lab

In this exercise, we're doing ETL on a file we've received from a customer. That file contains data about people, including:

* first, middle and last names
* gender
* birth date
* Social Security number
* salary

But, as is unfortunately common in data we get from this customer, the file contains some duplicate records. Worse:

* In some of the records, the names are mixed case (e.g., "Carol"), while in others, they are uppercase (e.g., "CAROL").
* The Social Security numbers aren't consistent either. Some of them are hyphenated (e.g., "992-83-4829"), while others are missing hyphens ("992834829").

The name fields are guaranteed to match, if you disregard character case, and the birth dates will also match. The salaries will match as well,
and the Social Security Numbers *would* match if they were somehow put in the same format.

Your job is to remove the duplicate records. The specific requirements of your job are:

* Remove duplicates. It doesn't matter which record you keep; it only matters that you keep one of them.
* Preserve the data format of the columns. For example, if you write the first name column in all lowercase, you haven't met this requirement.

Next, you will write the results using two methods:

* First, write the result as a Parquet file, as designated by *destFile*. It must contain 8 part files (8 files ending in ".parquet").
* Next, as is the best practice, write the result to Delta as designated by the filepath *deltaDestFile*.

<img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** The initial dataset contains 103,000 records.<br/>
The de-duplicated result has 100,000 records.

##### Methods
- DataFrameReader (<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameReader.html" target="_blank">Scala</a>)
- DataFrame (<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html" target="_blank">Scala</a>)
- Built-In Functions (<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html?#functions" target="_blank">Python</a>/<a href="http://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">Scala</a>)
- DataFrameWriter (<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">Python</a>/<a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameWriter.html" target="_blank">Scala</a>)*/

-- iniciando aula
%run ./Includes/Classroom-Setup

-- revisando archivo
%fs head dbfs:/mnt/training/dataframes/people-with-dups.txt

-- importando archivo
# TODO

sourceFile = "dbfs:/mnt/training/dataframes/people-with-dups.txt"
destFile = workingDir + "/people.parquet"

# In case it already exists
dbutils.fs.rm(destFile, True)

# dropDuplicates() will likely introduce a shuffle, so it helps to reduce the number of post-shuffle partitions.
spark.conf.set("spark.sql.shuffle.partitions", 8)

df = (spark.read
     .option("sep", ":")
     .option("header", True)
     .option("inferSchema", True)
     .csv(sourceFile))

display(df.count())

-- borrando duplicados
from pyspark.sql.functions import *

dedupedDF = (df.withColumn("firstName", initcap(col("firstName")))
             .withColumn("middleName", initcap(col("middleName")))
             .withColumn("lastName", initcap(col("lastName")))
             .withColumn("ssn", regexp_replace("ssn", "-", ""))#translate(col("ssn"), "-", "").alias("ssnNums")
             .distinct()
)

display(dedupedDF.count())

-- exportando archivo
(dedupedDF.repartition(8)
   .write
   .mode("overwrite")
   .option("compression", "snappy")
   .parquet(destFile)
)
dedupedDF = spark.read.parquet(destFile)
#print(f"Total Records: {dedupedDF.count():,}")

-- verificacion
partFiles = len(list(filter(lambda f: f.path.endswith(".parquet"), dbutils.fs.ls(destFile))))

finalDF = spark.read.parquet(destFile)
finalCount = finalDF.count()

assert partFiles == 8, "expected 8 parquet files located in destFile"
assert finalCount == 100000, "expected 100000 records in finalDF"
# Está usando el dilter de pyspark y no de python, queda pendiente de arreglar

-- cerrando clase
%run "./Includes/Classroom-Cleanup"
