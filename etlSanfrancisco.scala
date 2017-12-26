%spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.types
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.functions.year
import org.joda.time.Days
import org.joda.time.DateTime
import org.joda.time.LocalDate
//import com.github.nscala_time.time
//import com.github.nscala_time.time.Imports._
//import com.github.nscala_time.time.Imports.DateTime._
/*
val spark = SparkSession
    .builder()
    .appName("test")
    .enableHiveSupport()
    .getOrCreate()

import spark.implicits._
import spark.sql*/


var df = sqlContext.emptyDataFrame

def read_csv_Police_Department_Incidents():DataFrame = df
{
    var data = sqlContext.read.format("com.databricks.spark.csv")
                            .option("header","true")    
                            .option("inferSchema","true")
                            .option("delimiter",",")
                            .load("/home/alex/hbase/hfiles/testdf/Police_Department_Incidents.csv")
    df = data.limit(2000)
            .withColumn("month", month(to_date(data("Date"),"MM/dd/yyyy")))
            .withColumn("year",year(to_date(data("Date"),"MM/dd/yyyy")))   
            //.withColumn("id", monotonically_increasing_id())
}
/*
def read_csv_Fire_Calls_Service():DataFrame = df
{
    var data = sqlContext.read.format("com.databricks.spark.csv")
                            .option("header","true")    
                            .option("inferSchema","true")
                            .option("delimiter",",")
                            .load("/home/alex/hbase/hfiles/testdf/Fire_Department_Calls_for_Service.csv")
    data = data.limit(2000)
    df = data.withColumn("day", dayofmonth(to_date(data("Call Date"),"MM/dd/yyyy")))
            .withColumn("month", month(to_date(data("Call Date"),"MM/dd/yyyy")))
            .withColumn("year",year(to_date(data("Call Date"),"MM/dd/yyyy")))
            .withColumn("dayofweek", date_format(to_date(data("Call date"), "MM/dd/yyyy"),"EEEE"))
}*/
/*
def save_csv(data: DataFrame, nameTable: String)
{
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    data.registerTempTable("tempTable")
    hc.sql("create table " + nameTable + " as select * from tempTable")
    
    val result = hc.sql("show tables")
    result.show()
}*/
/*
def save_csv2(data: DataFrame)
{
    data.registerTempTable("temp")
    sql("CREATE TABLE IF NOT EXISTS src as select * from temp")
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    val result = hc.sql("show tables")
    result.show()
}*/
/*
def write_csv_fire(data: DataFrame)
{
    data.coalesce(1)
        .write.format("com.databricks.spark.csv")
        .option("header","true")
        .save("home/alex/hbase/hfiles/testdf/Fire_Test")
}*/
def write_csv_police(data: DataFrame)
{
    data.coalesce(1)
        .write.format("com.databricks.spark.csv")
        .option("header","true")
        .save("/home/alex/hbase/hfiles/testdf/Police_Incidents_Test")
}

def get_frecuencia_de_llamada_por_tipo_fire(data: DataFrame)
{
    var dfView = sqlContext.emptyDataFrame
    
    dfView = data.select("Call Date", "Call Type")
                .groupBy("Call Date", "Call Type")
                .count()
}

def get_frecuencia_de_llamada_por_barrio_fire(data: DataFrame)
{
    var dfView = sqlContext.emptyDataFrame
    
    dfView = data.select("Call Date", "Call Type", "Neighborhooods - Analysis Boundaries", "month","year","dayofweek")
                .groupBy("Neighborhooods - Analysis Boundaries", "Call Type", "year","month")
                .count()
}

def get_evolucion_de_llamadas(data:DataFrame)
{
    var dfView = sqlContext.emptyDataFrame
    
    dfView = data.select("year","month","Call Type")
                .groupBy("year","month", "Call Type")
                .count()
}
/* Tiempo medio de respuesta por tipo de llamada y barrio*/
def get_tiempo_medio_respuesta(data:DataFrame)
{
    var dfView = sqlContext.emptyDataFrame
    
    dfView = data.filter(col("On Scene DtTm").isNotNull)
                .withColumn("responseTime", unix_timestamp(data("On Scene DtTm"), "MM/dd/yyyy hh:mm:ss aa") - unix_timestamp(data("Received DtTm"), "MM/dd/yyyy hh:mm:ss aa"))
                .select("Call Type","Neighborhooods - Analysis Boundaries","responseTime")
                .groupBy("Call Type", "Neighborhooods - Analysis Boundaries")
                .avg("responseTime")
}

/* Batallones de bomberos por tipo de llamada*/
def get_batallones_por_tipo_de_llamada(data: DataFrame)
{
    var dfView = sqlContext.emptyDataFrame
    
    dfView = data.select("Battalion", "Call Type")
                .groupBy("Battalion", "Call Type")
                .count()
                
    dfView.show()
}

/*Tipos de indcidentes por barrios*/
def get_tipos_de_incidentes_por_barrio(data: DataFrame)
{
    var dfView = sqlContext.emptyDataFrame
    
    dfView = data.select("Category", "PdDistrict")
                .groupBy("Category","PdDistrict")
                .count()
    //dfView.show()
}
def get_detenidos_por_robos(data:DataFrame)
{
    var dfView = sqlContext.emptyDataFrame
    
    dfView = data.select("Category","Resolution")
                //.filter(data("Resolution").contains("ARREST, BOOKED"))
                .groupBy("Category","Resolution")
                .count()
    dfView.show()
}
def dimension_Categorias(data: DataFrame)//:DataFrame = df
{
    
    var dim = sqlContext.emptyDataFrame
    var dfView = sqlContext.emptyDataFrame
    dim = data.select("Category")
            .distinct()
            .withColumn("id",monotonically_increasing_id())
            .withColumn("Name Category", data("Category"))
   // dim.show()
    
    /*dfView = data.drop(data("X"))
                .drop(data("Y"))
                .drop(data("Address"))
                .drop(data("Location"))
                .drop(data("Resolution"))*/
    //dfView.show()
    dfView = data.join(dim, data.col("Category") === dim.col("Name Category"))
    
    dfView = dfView.drop("Category")
                    .drop("Name Category")
                    .withColumnRenamed( "id","idCategory")
                    //.drop(dfView)
    
    /*dfView = dfView.withColumn("idCategory", data("id"))
                .drop(data("Category"))*/
    dfView.show(2000)
            /*
            .withColumn("id",monotonically_increasing_id())
    //dim.show(2000)
    df = data.join(dim, data.col("Category").equalTo(dim.col("Category")))*/
    
}/*
def dimension_tiempo()
{
    var dfView = sqlContext.emptyDataFrame
    
    
    dfView = seq(as.Date("2015/1/1"),as.Date("2016/1/1"),"day")
    dfView.show()
}*/
def dateRange()
{
   val numberOfDays = Days.daysBetween("1/1/2015", "1/1/2016").getDays()
    for (f<- 0 to numberOfDays) yield from.plusDays(f)


}
dateRange("1/1/2015","1/1/2016",1)
/*
var dfFireDepartment = sqlContext.emptyDataFrame
dfFireDepartment = read_csv_Fire_Calls_Service()
write_csv_fire(dfFireDepartment)*/
/*
var dfPoliceDepartment = sqlContext.emptyDataFrame
dfPoliceDepartment = read_csv_Police_Department_Incidents()
write_csv_police(dfPoliceDepartment)*/
//save_csv(x)*/

/*var x = read_csv_Fire_Calls_Service()
get_frecuencia_de_llamada_por_tipo_fire(x)
get_frecuencia_de_llamada_por_barrio_fire(x)
get_evolucion_de_llamadas(x)
get_tiempo_medio_respuesta(x)
get_batallones_por_tipo_de_llamada(x)*/
//var x = read_csv_Police_Department_Incidents()
//get_tipos_de_incidentes_por_barrio(x)
//get_detenidos_por_robos(x)
//var y = sqlContext.emptyDataFrame
//dimension_Categorias(x)

//x.show()
