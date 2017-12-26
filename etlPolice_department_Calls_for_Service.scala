%spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.types
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.functions.year
import org.joda.time.Days
import org.joda.time.LocalDate
import org.joda.time.DateTime
import scala.collection.mutable.ListBuffer

var df = sqlContext.emptyDataFrame

def read_csv_Police_Department_Incidents():DataFrame = df
{
    var data = sqlContext.read.format("com.databricks.spark.csv")
                            .option("header","true")    
                            .option("inferSchema","true")
                            .option("delimiter",",")
                            .load("/home/alex/hbase/hfiles/testdf/Police_Department_Incidents.csv")
    df = data.limit(2000)
}

def create_hive_table(data: DataFrame, nameTable: String)
{
    val drop = "drop table if exists " + nameTable
    val create = "create table " + nameTable + " as select * from tempTable"
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    data.registerTempTable("tempTable")
    hc.sql(drop)
    hc.sql(create)
}

def dimension_Categorias(data: DataFrame)//:DataFrame = df
{
    var dim = sqlContext.emptyDataFrame
    var dfView = sqlContext.emptyDataFrame
    dim = data.select("Category")
            .distinct()
            .withColumn("id",monotonically_increasing_id())
            
    dfView = data.join(dim, data.col("Category") === dim.col("Category"))
    
    dfView = dfView.drop("Category")
                    .drop("Name Category")
                    .withColumnRenamed( "id","idCategory")
    //dfView.show(2000)
    create_hive_table(dim,"PD_DIM_CATEGORIA")
}

def dimension_Barrio(data:DataFrame)
{
    var dim = sqlContext.emptyDataFrame
    var dfView = sqlContext.emptyDataFrame
    dim = data.select("Address","PdDistrict")
                .distinct()
                .orderBy("PdDistrict","Address")
                .withColumn("id", monotonically_increasing_id())
                .withColumnRenamed("Address","direccion")
    dfView = data.join(dim,data.col("Address") === dim.col("direccion"))
    dfView = dfView.drop("direccion")
                    .drop("Address")
                    .drop("PdDistrict")
    //dfView.show()
    create_hive_table(dim,"PD_DIM_BARRIO")
}

def dimension_Fechas()
{
    var start = new LocalDate(2003,1,1)
    var end = new LocalDate(2019,1,1)
    val numberOfDays = Days.daysBetween(start, end).getDays()
    var fechas = for (f<- 0 to numberOfDays) yield start.plusDays(f).toString("yyyy/MM/dd")
    var listaFechas = fechas.toList
    var dfFechas = sqlContext.emptyDataFrame
    dfFechas = listaFechas.toDF
    dfFechas = dfFechas.withColumnRenamed("value","date")
    dfFechas = dfFechas.withColumn("year",year(to_date(dfFechas("date"),"yyyy/MM/dd")))
                        .withColumn("month", month(to_date(dfFechas("date"),"yyyy/MM/dd")))
                        .withColumn("day", dayofmonth(to_date(dfFechas("date"),"yyyy/MM/dd")))
                        .withColumn("dayofweek", date_format(to_date(dfFechas("date"), "yyyy/MM/dd"),"EEEE"))
                        .withColumn("id", monotonically_increasing_id())
    //dfFechas.show()
    create_hive_table(dfFechas,"PD_DIM_FECHAS")
}

def dimension_Resoluciones(data:DataFrame)
{
    var dim = sqlContext.emptyDataFrame
    var dfView = sqlContext.emptyDataFrame
    dim = data.select("Resolution")
                .distinct()
                .withColumn("id", monotonically_increasing_id())
    dfView = data.join(dim,data.col("Resolution") === dim.col("Resolution"))
    dfView = dfView.drop("Resolution")
                    .withColumnRenamed("id", "idResolution")
    //dfView.show()
    create_hive_table(dim,"PD_DIM_RESOLUCIONES")
}

def dimension_Horas()
{
    var listaHoras = List[String]()
    var hora = ""
    var minutos = ""
    var aux = ""
    var dfHoras = sqlContext.emptyDataFrame
    for (i<- 0 to 23) 
    {
        for(j<-0 to 59)
        {
            if(i < 10)
                hora = "0" + i.toString()
            else
                hora = i.toString()
            
            if(j < 10)
                minutos = "0" + j.toString()
            else
                minutos = j.toString
            aux = hora + ":" + minutos
            listaHoras = listaHoras ::: List(aux)
        }
    }
    dfHoras = listaHoras.toDF
    dfHoras = dfHoras.withColumnRenamed("value","horas")
    dfHoras = dfHoras.withColumn("id", monotonically_increasing_id())
                    .withColumn("hora",hour(dfHoras("horas")))
                    .withColumn("minutos",minute(dfHoras("horas")))
    //dfHoras.show(2000)
    create_hive_table(dfHoras,"PD_DIM_HORAS")
}

def factTable(data:DataFrame)
{
    var factTable = data.drop("X")
                        .drop("Y")
                        .drop("DayOfWeek")
                        .drop("PdDistrict")
                        .drop("PdId")
                        .drop("Descript")
    
    //var data = read_csv_Police_Department_Incidents()
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    
    //Dimension Categoria
    var dimension_categoria = hc.sql("select id, Category from pd_dim_categoria")
    factTable = factTable.join(dimension_categoria, factTable.col("Category") === dimension_categoria.col("Category"))
    factTable = factTable//.drop("Category")
                        .withColumnRenamed("id","idCategory")
                        .drop("Category")
    //Dimension Fecha
    var dimension_fecha = hc.sql("select id,date from pd_dim_fechas")
    
    factTable = factTable.join(dimension_fecha, unix_timestamp(factTable.col("Date"),"MM/dd/yyyy") === unix_timestamp(dimension_fecha.col("date"),"yyyy/MM/dd"))
    factTable = factTable.withColumnRenamed("id","idFecha")
                        .drop("Date")
                        .drop("date")
    
    //Dimension Resolution
    var dimension_resolution = hc.sql("select id, Resolution as resolucion from pd_dim_resoluciones")
    factTable = factTable.join(dimension_resolution, factTable.col("Resolution") === dimension_resolution.col("Resolucion"))
    
    factTable = factTable.withColumnRenamed("id", "idResolution")
                        .drop("Resolution")
                        .drop("resolucion")
    //Dimension Barrio
    var dimension_barrio = hc.sql("select direccion, id from pd_dim_barrio")
    factTable = factTable.join(dimension_barrio,factTable.col("Address") === dimension_barrio.col("direccion"))
    
    factTable = factTable.withColumnRenamed("id","idDireccion")
                        .drop("direccion")
                        .drop("Address")
    //Dimension hora
    var dimension_hora = hc.sql("select id,horas from pd_dim_horas")
    factTable = factTable.join(dimension_hora,factTable.col("Time") === dimension_hora.col("horas"))
    
    factTable = factTable.withColumnRenamed("id","idTime")
                        .drop("Time")
                        .drop("horas")
    
    //Medida NumDelitos
    var dfView = sqlContext.emptyDataFrame
    dfView = factTable.groupBy("IncidntNum")
                    .count()
    dfView = dfView.withColumnRenamed("incidntNum", "idIncidnt")
                    .withColumnRenamed("count","numIncidnt")
    factTable = factTable.join(dfView, factTable.col("IncidntNum") === dfView.col("idIncidnt"))
    
    factTable = factTable.drop("idIncidnt")
    create_hive_table(factTable,"PD_FACTTABLE")
}

var x = read_csv_Police_Department_Incidents()
dimension_Categorias(x)
dimension_Barrio(x)
dimension_Fechas()
dimension_Resoluciones(x)
dimension_Horas()
factTable(x)

