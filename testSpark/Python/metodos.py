%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, concat, count, countDistinct,
                                   current_timestamp, date_format, desc, floor,
                                   lag, lit, monotonically_increasing_id, sum,
                                   udf, unix_timestamp)
from pyspark.sql.types import FloatType, StringType
from pyspark.sql.window import Window
from pyspark.sql import SQLContext

class test:

    app = None
    dataframes = None

    #METODO 1
    def get_top_clientes_por_importe_total(data, top_count=10):
        #Para pasar de decimal a Euro(metodo no utilizado)
        decimal_to_euro = udf(
            lambda num: '{0:,.2f} €'.format(num), StringType())
        df = None
        if(top_count is None):
            top_count = 10
        #operacion SQL
        if(data is not None):
            df = (data
                    .groupBy('customerID')
                    .agg(sum('Total').alias('Importe Total'))
                    .sort(desc('Importe Total'))
                    .limit(top_count)
                )
        return df
    #   METODO 2
    def get_distribucion_de_clientes_por_importe(data, top_count=10):
        decimal_to_euro = udf(
            lambda num: '{0:,.2f} €'.format(num), StringType())
        df_view = None
        
        sqlcontext = SQLContext(sc)
        if top_count is None:
            top_count = 10

        if(data is not None):
            clientes = (data
                            .groupBy('CustomerID')
                            .agg(sum('Total').alias('Importe Total'))
                            .sort(desc('Importe Total'))
                            .cache())
            
            clientes_top10 = clientes.limit(top_count)
            nottop10_total = clientes.agg(sum('Importe Total')).collect()[0][
                0] - clientes_top10.agg(sum('Importe Total')).collect()[0][0]
            df_nottop10_data = [('Resto de clientes', nottop10_total)]
            df_nottop10 = sqlcontext.createDataFrame(
                                df_nottop10_data, schema=clientes_top10.schema)
            df_view = (clientes_top10
                       .select('CustomerID', 'Importe Total'))

            df_view = df_view.union(df_nottop10)

            return df_view
            
    #METODO 3      
    def get_porcentaje_de_retencion_de_clientes(data, time_period=2592000):
        result = 0
        if(data is not None):
            time_from = current_timestamp().astype('bigint')
            base = data.select('CustomerID', 'InvoiceDate')\
                .where((time_from - unix_timestamp(col('InvoiceDate'), 'dd/MM/yyyy HH:mm')) < time_period)
		#Aquellos que aparecen 2 o mas veces son retained
            retained = base.groupBy('CustomerID').agg(count('InvoiceDate').alias(
                'Interacciones')).where(col('Interacciones') >= 2).count()
            total = 0
            total = base.agg(countDistinct('CustomerID')).collect()[0][0]

            if(total == 0):
                total = 1

            result = (float(retained) / float(total)) * 100

        return result
    
    #METODO 5
    def get_distribucion_de_clientes_por_facturas_emitidas(data):
        df = None
        if(data is not None):
            df = (data
                    #InvoiceID equivale a InvoiceNo
                    .select('CustomerID', 'InvoiceNo', 'Total')
                    .groupBy('CustomerID')
                    .agg(count('InvoiceNo').alias('facturas'), sum('Total').alias('Total'))
                    .withColumn('facturas', floor(col('facturas') / 10) * 10)
                    .groupBy('facturas')
                    .agg(count('CustomerID').alias('Clientes'))
                    .sort('facturas'))
            return df
        
    #METODO 6
    def get_distribucion_de_beneficios_por_facturas_emitidas(data):
        df = None
        if(data is not None):
            df = (data
                        .select('CustomerID', 'InvoiceNo', 'Total')
                        .groupBy('CustomerID')
                        .agg(count('InvoiceNo').alias('facturas'),
                            sum('Total').alias('Total'))
                        .withColumn('facturas', floor(col('facturas') / 10) * 10)
                        .groupBy('facturas')
                        .agg(sum('Total').alias('Importe Total'))
                        .sort('facturas')
                )
        return df
    
