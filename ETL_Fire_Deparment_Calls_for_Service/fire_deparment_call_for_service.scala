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

def read_csv_Fire_Department_Calls_for_Service():DataFrame = df
{
    var data = sqlContext.read.format("com.databricks.spark.csv")
                            .option("header","true")    
                            .option("inferSchema","true")
                            .option("delimiter",",")
                            .load("/home/alex/hbase/hfiles/Fire_Department_Calls_for_Service.csv")
    df = data//.limit(2000)
            .drop("Call Number")
            .drop("Watch Date")
            .drop("Entry DtTm")
            .drop("Transport DtTm")
            .drop("City")
            .drop("Hospital DtTm")
            .drop("Zipcode of Incident")
            .drop("Station Area")
            .drop("Box")
            .drop("Original Priority")
            .drop("Priority")
            .drop("Final Priority")
            .drop("ALS Unit")
            .drop("Number of Alarms")
            .drop("Unit sequence in call dispatch")
            .drop("Fire pPrevention District")
            .drop("Supervisor District")
            .drop("RowID")
            .drop("Fire Prevention District")
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

def dimension_Localizacion(data: DataFrame)//:DataFrame = df
{
    var dim = sqlContext.emptyDataFrame
    dim = data.select("Address", "Neighborhooods - Analysis Boundaries")
            .distinct()
            .withColumnRenamed("Neighborhooods - Analysis Boundaries","barrio")
            .orderBy("barrio", "Address")
            .withColumn("id",monotonically_increasing_id())
    create_hive_table(dim,"FIRE_DIM_LOCATION")
}

def dimension_Unidad(data:DataFrame)
{
    var dim = sqlContext.emptyDataFrame
    dim = data.select("Unit ID", "Unit Type")
                .distinct()
                .orderBy("Unit ID","Unit Type")
                .withColumn("id", monotonically_increasing_id())
                .withColumnRenamed("Unit ID", "UnitID")
                .withColumnRenamed("Unit Type", "UnitType")
    create_hive_table(dim,"FIRE_DIM_UNIT")
}

def dimension_TipoLlamada(data:DataFrame)
{
    var dim = sqlContext.emptyDataFrame
    dim = data.select("Call Type", "Call Type Group")
                .distinct()
                .orderBy("Call Type")
                .withColumn("id", monotonically_increasing_id())
                .withColumnRenamed("Call Type", "callType")
                .withColumnRenamed("Call Type Group", "callTypeGroup")
    create_hive_table(dim,"FIRE_DIM_CALLTYPE")

}

def dimension_CallFinalDisposition(data:DataFrame)
{
    var dim = sqlContext.emptyDataFrame
    dim = data.select("Call Final Disposition")
                .distinct()
                .orderBy("Call Final Disposition")
                .withColumn("id", monotonically_increasing_id())
                .withColumnRenamed("Call Final Disposition", "CallFinalDisposition")
    create_hive_table(dim,"FIRE_DIM_CALLFINALDISPOSITION")
}

def dimension_Battalion(data:DataFrame)
{
    var dim = sqlContext.emptyDataFrame
    dim = data.select("Battalion")
                .distinct()
                .orderBy("Battalion")
                .withColumn("id", monotonically_increasing_id())
    create_hive_table(dim,"FIRE_DIM_BATTALION")
}

def fire_factTable(data:DataFrame)
{
    var factTable = data.drop("Neighborhooods - Analysis Boundaries")
                        .drop("UnitType")
                        .drop("Call Type Group")
                        .drop("Unit Type")
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    
    //Dimension Localizacion
    var dimension_localizacion = hc.sql("select id, Address from fire_dim_location")
    factTable = factTable.join(dimension_localizacion, factTable.col("Address") === dimension_localizacion.col("Address"))
    factTable = factTable.drop("Address")
                        .withColumnRenamed("id", "idAddress")

    
    //Dimension Unidad
    var dimension_unidad = hc.sql("select id, unitID from fire_dim_unit")
    factTable = factTable.join(dimension_unidad, factTable.col("Unit ID") === dimension_unidad.col("UnitID"))
    factTable = factTable.drop("Unit ID")
                        .drop("UnitID")
                        .withColumnRenamed("id", "idUnit")
    
    //Dimension tipo de llamada
    var dimension_callType = hc.sql("select id, callType from fire_dim_calltype")
    factTable = factTable.join(dimension_callType, factTable.col("Call Type") === dimension_callType("callType"))
    factTable = factTable.drop("Call Type")
                        .drop("callType")
                        .withColumnRenamed("id", "idCallType")
    //Dimension Disposicion final de la llamada
    var dimension_callFinalDisposition = hc.sql("select id, callFinalDisposition from fire_dim_callfinaldisposition")
    factTable = factTable.join(dimension_callFinalDisposition, factTable.col("Call Final Disposition") === dimension_callFinalDisposition("callFinalDisposition"))
    factTable = factTable.drop("Call Final Disposition")
                        .drop("callFinalDisposition")
                        .withColumnRenamed("id", "callFinalDisposition")
    //Dimension Battalion
    var dimension_battalion = hc.sql("select id, battalion from fire_dim_battalion")
    factTable = factTable.join(dimension_battalion, factTable.col("Battalion") === dimension_battalion("battalion"))
    factTable = factTable.drop("Battalion")
                        .drop("battalion")
                        .withColumnRenamed("id", "idBattalion")
    
    //Dimension Fecha
    var dimension_fecha = hc.sql("select id,date from dim_fechas")
    
    factTable = factTable.join(dimension_fecha, unix_timestamp(factTable.col("Call Date"),"MM/dd/yyyy") === unix_timestamp(dimension_fecha.col("date"),"yyyy/MM/dd"))
    factTable = factTable.withColumnRenamed("id","idFecha")
                        .drop("Date")
                        .drop("date")
                        
    //Medidas
    factTable = factTable.withColumn("tiempoRespuesta", 
                            when(factTable.col("On Scene DtTm").isNotNull,
                                unix_timestamp(factTable("On Scene DtTm"), "MM/dd/yyyy hh:mm:ss aa") - unix_timestamp(factTable("Received DtTm"), "MM/dd/yyyy hh:mm:ss aa")))
                        .withColumn("tiempoDisponible",
                            unix_timestamp(factTable("Available DtTm"), "MM/dd/yyyy hh:mm:ss aa") - unix_timestamp(factTable("Dispatch DtTm"), "MM/dd/yyyy hh:mm:ss aa"))
    //Limpieza de campos
    factTable = factTable.drop("Received DtTm")
                        .drop("On Scene DtTm")
                        .drop("Available DtTm")
                        .drop("Dispatch DtTm")
                        .drop("Response DtTm")
                        .drop("Call Date")
    
        create_hive_table(factTable,"FIRE_FACTTABLE")
      // factTable.show(5000)   
}

var fire = read_csv_Fire_Department_Calls_for_Service()
dimension_Localizacion(fire)
dimension_Unidad(fire)
dimension_TipoLlamada(fire)
dimension_CallFinalDisposition(fire)
dimension_Battalion(fire)
fire_factTable(fire)


