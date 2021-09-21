-- abriendo la clase
%run ./Includes/Classroom-Setup

-- seleccionando el user_id y el datetime del evento
from pyspark.sql.functions import col

df = spark.read.parquet(eventsPath).select("user_id", col("event_timestamp").alias("timestamp"))
display(df)

-- casteando el timestamp (timestamp)
timestampDF = df.withColumn("timestamp", (col("timestamp") / 1e6).cast("timestamp"))
display(timestampDF)

-- casteando el timestamp (timestamptype)
from pyspark.sql.types import TimestampType

timestampDF = df.withColumn("timestamp", (col("timestamp") / 1e6).cast(TimestampType()))
display(timestampDF)

-- extrayendo la fecha y hora en nuevas columnas con un patron de fecha
from pyspark.sql.functions import date_format

formattedDF = (timestampDF.withColumn("date string", date_format("timestamp", "MMMM dd, yyyy"))
  .withColumn("time string", date_format("timestamp", "HH:mm:ss.SSSSSS"))
)
display(formattedDF)

-- extrayendo año, mes, dia de la semana, minuto, segundo en nuevas columnas
from pyspark.sql.functions import year, month, dayofweek, minute, second

datetimeDF = (timestampDF.withColumn("year", year(col("timestamp")))
  .withColumn("month", month(col("timestamp")))
  .withColumn("dayofweek", dayofweek(col("timestamp")))
  .withColumn("minute", minute(col("timestamp")))
  .withColumn("second", second(col("timestamp")))
)
display(datetimeDF)

-- obteniendo la fecha de un timestamp (función)
from pyspark.sql.functions import to_date

dateDF = timestampDF.withColumn("date", to_date(col("timestamp")))
display(dateDF)

-- agregando dias a una fecha
from pyspark.sql.functions import date_add

plus2DF = timestampDF.withColumn("plus_two_days", date_add(col("timestamp"), 2))
display(plus2DF)
