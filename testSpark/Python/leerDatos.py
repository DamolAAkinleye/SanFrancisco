%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, concat, count, countDistinct,
                                   current_timestamp, date_format, desc, floor,
                                   lag, lit, monotonically_increasing_id, sum,
                                   udf, unix_timestamp)
from pyspark.sql.types import FloatType, StringType
from pyspark.sql.window import Window
from pyspark.sql import SQLContext
import time

#Clase APP, lee un csv a RDD; lo convierte a dataframe y le añade la columna Total
class APP:
    
    data = None
    def loadCSV():
        #Leemos CSV a RDD
        test = sqlContext.read.csv("/home/alex/Escritorio/CodigoYDatos/billing.csv",header=True)
        #De RDD a Dataframe
        df = test.toDF('InvoiceNo','StockCode','Description','Quantity','InvoiceDate', 'UnitPrice', 'CustomerID','Country')
        #Cambio del tipo de variable de algunas columnas(por defecto todas son string)
        df = df.withColumn('UnitPrice', df["UnitPrice"].cast("float"))
        df = df.withColumn('Quantity', df["Quantity"].cast("int"))
        df = df.withColumn('CustomerID', df["CustomerID"].cast("int"))
        #Añadimos columna Total(unitPrice*Quantity)
        dfmod = df.withColumn('Total', df["UnitPrice"] * df["Quantity"])
        return dfmod
        
    #Metodo que devuelve un dataframe a partir de un csv
    def get_datos():
        #Leemos CSV a RDD
        test = sqlContext.read.csv("/home/alex/Escritorio/CodigoYDatos/billing.csv",header=True)
        #De RDD a Dataframe
        df = test.toDF('InvoiceNo','StockCode','Description','Quantity','InvoiceDate', 'UnitPrice', 'CustomerID','Country')
        #Cambio del tipo de variable de algunas columnas(por defecto todas son string)
        df = df.withColumn('UnitPrice', df["UnitPrice"].cast("float"))
        df = df.withColumn('Quantity', df["Quantity"].cast("int"))
        #Añadimos columna Total(unitPrice*Quantity)
        dfmod = df.withColumn('Total', df["UnitPrice"] * df["Quantity"])
        return dfmod
    
    #Otra manera de leer csv a dataframe
    def get_datos2():
        test = sc.textFile("/home/alex/Escritorio/CodigoYDatos/billing.csv").map(lambda line: line.split(","))
        test_df = test.toDF(['InvoiceNo','StockCode','Description','Quantity','InvoiceDate', 'UnitPrice', 'CustomerID','Country'])
        return test_df

      


