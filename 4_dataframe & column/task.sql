eventsDF = spark.read.parquet(eventsPath)
display(eventsDF)
