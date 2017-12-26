%spark
var fecha = List("2017-04-08 1:57:02")
var test = fecha.toDF
var fecha2 = List("2017-04-08 1:57:02 PM")
var test2 = fecha2.toDF
var x = spark.sql("SELECT from_unixtime(unix_timestamp('01-01-2017 1:59:59 PM', 'dd-MM-yyyy hh:mm:ss aa'), 'dd-MM-yyyy HH:mm:ss')")
//var z = test2.withColumn("fecha", date_format(to_date(test2("value"),"yyyy-MM-dd HH:mm aa"),"yyyy-MM-dd HH:mm"))
var y = test2.withColumn("fecha", unix_timestamp(test2("value"), "yyyy-MM-dd hh:mm:ss aa"))
var z = test.withColumn("fecha", unix_timestamp(test("value"), "yyyy-MM-dd hh:mm:ss"))
y.show()
//z.show()

println(y)

%spark
 var start = new LocalDate(2003,1,1)
    var end = new LocalDate(2019,1,1)
    val numberOfDays = Days.daysBetween(start, end).getDays()
    var fechas = for (f<- 0 to numberOfDays) yield start.plusDays(f).toString("yyyy/MM/dd")

