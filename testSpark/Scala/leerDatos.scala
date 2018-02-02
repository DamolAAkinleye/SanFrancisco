%spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.types
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.year

    
    def get_datos2(): DataFrame =
    {
        var data2 = sqlContext.read.csv("/home/alex/Escritorio/CodigoYDatos/billing.csv")
        var df_aux = data2.toDF("InvoiceNo","StockCode","Description","Quantity","InvoiceDate", "UnitPrice", "CustomerID","Country")
        return df_aux
    }
    def get_datos(): DataFrame = 
    {
        var data = sqlContext.read.format("com.databricks.spark.csv")
                            .option("header","true")
                            .option("inferSchema","true")
                            .load("/home/alex/Escritorio/CodigoYDatos/billing.csv")
        data = data.withColumn("UnitPrice",data("UnitPrice").cast("Float"))
        data = data.withColumn("Quantity",data("Quantity").cast("Int"))
        data = data.withColumn("CustomerID",data("CustomerID").cast("Int"))
        //data = data.withColumn("InvoiceDate",data("InvoiceDate").cast("Date"))
        var df = data.withColumn("Total", data("UnitPrice") * data("Quantity"))
        return df                 
    }
     /* Otra manera de leer Dataframes     */                 
    def get_datos3(): DataFrame = 
    {
       var data3 = spark.read.format("csv")
                                .option("header", "true") //reading the headers
                                .option("mode", "DROPMALFORMED")
                                .load("/home/alex/Escritorio/CodigoYDatos/billing.csv")
        return data3
    }
    
    def get_datos4(): DataFrame = 
    {
        var data = sqlContext.read.format("com.databricks.spark.csv")
                            .option("header","true")
                            .option("inferSchema","true")
                            .load("/home/alex/Escritorio/CodigoYDatos/billing.csv")
        return data                 
    }
var c = get_datos()
c.show()
