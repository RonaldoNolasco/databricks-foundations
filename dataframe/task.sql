-- creando un df (spark)
eventsDF = spark.table("events")

-- pintando df
eventsDF.show()
display(eventsDF)

-- transformaciones al df
macDF = (eventsDF
  .where("device = 'macOS'")
   .orderBy("event_timestamp")
)
display(macDF)

-- contando y obteniendo el top del df
numRows = macDF.count()
rows = macDF.take(5)

-- comprobando los calculos anteriores
from pyspark.sql import Row

assert(numRows == 1938215)
assert(len(rows) == 5)
assert(type(rows[0]) == Row)

-- creando df (con sql)
macSQLDF = spark.sql("select * from events where device = 'macOS' order by event_timestamp")
display(macSQLDF)

-- cerrando el aula
%run ./Includes/Classroom-Cleanup
