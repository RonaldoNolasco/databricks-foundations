/*Sort Day Lab
1. Define UDF to label day of week
1. Apply UDF to label and sort by day of week
1. Plot active users by day of week as bar graph*/
/*%md
Start with a DataFrame of the average number of active users by day of week.

This was the resulting `df` in a previous lab.*/
from pyspark.sql.functions import approx_count_distinct, avg, col, date_format, to_date

df = (spark.read.parquet(eventsPath)
  .withColumn("ts", (col("event_timestamp") / 1e6).cast("timestamp"))
  .withColumn("date", to_date("ts"))
  .groupBy("date").agg(approx_count_distinct("user_id").alias("active_users"))
  .withColumn("day", date_format(col("date"), "E"))
  .groupBy("day").agg(avg(col("active_users")).alias("avg_users")))

display(df)

/*%md
### 1. Define UDF to label day of week
- Use the **`labelDayOfWeek`** provided below to create the udf **`labelDowUDF`***/
def labelDayOfWeek(day):
  dow = {"Mon": "1", "Tue": "2", "Wed": "3", "Thu": "4",
         "Fri": "5", "Sat": "6", "Sun": "7"}
  return dow.get(day) + "-" + day

-- creando la user defined function (UDF)
labelDowUDF = udf(labelDayOfWeek)

/*%md
### 2. Apply UDF to label and sort by day of week
- Update the **`day`** column by applying the UDF and replacing this column
- Sort by **`day`**
- Plot as bar graph*/
finalDF = df.withColumn("day", labelDowUDF(col("day"))).sort(col("day"))

display(finalDF)

-- cerrando el aula
%run ./Includes/Classroom-Cleanup
