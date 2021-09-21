-- creando el dataframe
df = (spark.read.parquet(eventsPath)
  .select("user_id", col("event_timestamp").alias("ts")))

display(df)

/*### 1. Extract timestamp and date of events
- Convert **`ts`** from microseconds to seconds by dividing by 1 million and cast to timestamp
- Add **`date`** column by converting **`ts`** to date*/
datetimeDF = (df.withColumn("ts", (col("ts") / 1e6).cast("timestamp"))
  .withColumn("date", to_date(col("ts")))
)
display(datetimeDF)

-- verificacion
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

expected1a = StructType([StructField("user_id", StringType(), True),
  StructField("ts", TimestampType(), True),
  StructField("date", DateType(), True)])

result1a = datetimeDF.schema

assert expected1a == result1a, "datetimeDF does not have the expected schema"

-- verificacion 2
import datetime

expected1b = datetime.date(2020, 6, 19)
result1b = datetimeDF.sort("date").first().date

assert expected1b == result1b, "datetimeDF does not have the expected date values"

/*%md
### 2. Get daily active users
- Group by date
- Aggregate approximate count of distinct **`user_id`** and alias with "active_users"
  - Recall built-in function to get approximate count distinct
- Sort by date
- Plot as line graph*/

from pyspark.sql.functions import avg, approx_count_distinct
activeUsersDF = (datetimeDF.groupBy("date")
                 .agg(approx_count_distinct("user_id").alias("active_users"))
                 .sort("date")                
)
display(activeUsersDF)

-- verificacion
from pyspark.sql.types import LongType

expected2a = StructType([StructField("date", DateType(), True),
  StructField("active_users", LongType(), False)])

result2a = activeUsersDF.schema

assert expected2a == result2a, "activeUsersDF does not have the expected schema"

-- verificacion
expected2b = [(datetime.date(2020, 6, 19), 251573), (datetime.date(2020, 6, 20), 357215), (datetime.date(2020, 6, 21), 305055), (datetime.date(2020, 6, 22), 239094), (datetime.date(2020, 6, 23), 243117)]

result2b = [(row.date, row.active_users) for row in activeUsersDF.take(5)]

assert expected2b == result2b, "activeUsersDF does not have the expected values"

/*%md
### 3. Get average number of active users by day of week
- Add **`day`** column by extracting day of week from **`date`** using a datetime pattern string
- Group by **`day`**
- Aggregate average of **`active_users`** and alias with "avg_users"*/

activeDowDF = (activeUsersDF.withColumn("day", date_format("date", "E"))
               .groupBy("day")
               .agg(avg("active_users").alias("avg_users"))
)
display(activeDowDF)

-- verificacion
from pyspark.sql.types import DoubleType

expected3a = StructType([StructField("day", StringType(), True),
  StructField("avg_users", DoubleType(), True)])

result3a = activeDowDF.schema

assert expected3a == result3a, "activeDowDF does not have the expected schema"

--verificacion
expected3b = [('Fri', 247180.66666666666), ('Mon', 238195.5), ('Sat', 278482.0), ('Sun', 282905.5), ('Thu', 264620.0), ('Tue', 260942.5), ('Wed', 227214.0)]

result3b = [(row.day, row.avg_users) for row in activeDowDF.sort("day").collect()]

assert expected3b == result3b, "activeDowDF does not have the expected values"

-- cerrando el aula
%run ./Includes/Classroom-Cleanup



