%spark
import org.joda.time.LocalDate
import org.joda.time.DateTime

var start = new LocalDate(2015,1,1)
var end = new LocalDate(2016,1,1)
val numberOfDays = Days.daysBetween(start, end).getDays()
var fechas = for (f<- 0 to numberOfDays) yield start.plusDays(f).toString()
var fin = fechas.toList
var datafra = sqlContext.emptyDataFrame
datafra = fin.toDF
datafra.show()
