%pyspark
#from lucentia.utils.utils import getSparkSQL
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, concat, count, countDistinct,
                                   current_timestamp, date_format, desc, floor,
                                   lag, lit, monotonically_increasing_id, sum,
                                   udf, unix_timestamp)
from pyspark.sql.types import FloatType, StringType
from pyspark.sql.window import Window
from pyspark.sql import SQLContext

class Clientes:

    app = None
    dataframes = None

    def __init__(self, app):
        #self.sparkSQL = getSparkSQL()
        self.app = app
        self.dataframes = {}

    def debug(self):

        print("%html <h1>Class Clientes</h1>")
        print("%html <h2>Clientes.debug()</h2>")
        print("%%html <h3>Clientes.debug() -> self.dataframes</h3><p>%s</p>" %
              "<br>".join(self.dataframes.keys()))
        print("%html <h3>Clientes.debug() -> Dataframes</h3>")

        for key, value in self.dataframes.items():
            print("%%html <h4>Dataframe: %(keys)</h4> <pre>" % locals())
            value.limit(3).show()

    def expose(self, df_name, df):
        self.dataframes[df_name] = df
        self.app.write_dataframe_file(self.dataframes[df_name], df_name)

    #METODO 1
    def get_top_clientes_por_importe_total(self, top_count=10):
        #Para pasar de decimal a Euro(metodo no utilizado)
        decimal_to_euro = udf(
            lambda num: '{0:,.2f} €'.format(num), StringType())
        df = None
        if(top_count is None):
            top_count = 10
        #operacion SQL
        if(self is not None):
            df = (self.app.get_invoice_lines_positive()
                    .groupBy('customerID')
                    .agg(sum('Total').alias('Importe Total'))
                    .sort(desc('Importe Total'))
                    .limit(top_count)
                  #.withColumn('Posicion', monotonically_increasing_id() + 1) # Omitido para facilitar la representacion
                )
        return df
    #   METODO 2
    def get_distribucion_de_clientes_por_importe(self, top_count=10):
        decimal_to_euro = udf(
            lambda num: '{0:,.2f} €'.format(num), StringType())
        df_view = None
        
        sqlcontext = SQLContext(sc)
        if top_count is None:
            top_count = 10

        if(self is not None):
            gb_clients = (self.app.get_invoice_lines_positive()
                            .groupBy('CustomerID')
                            .agg(sum('Total').alias('Importe Total'))
                            .sort(desc('Importe Total'))
                            .cache())
            
            gb_clients_top10 = gb_clients.limit(top_count)
            # perhaps calculating totals instead of operating over rows it is
            # faster?
            nottop10_total = gb_clients.agg(sum('Importe Total')).collect()[0][
                0] - gb_clients_top10.agg(sum('Importe Total')).collect()[0][0]
            df_nottop10_data = [('Resto de clientes', nottop10_total)]
            #sparkSQL.createDataFrame no existe, pertenece a lucentialab
            df_nottop10 = sqlcontext.createDataFrame(
                                df_nottop10_data, schema=gb_clients_top10.schema)
            #df_nottop10 = df_nottop10_data.toDF(gb_clients_top10.schema)

            df_view = (gb_clients_top10#.withColumn('Posicion', monotonically_increasing_id() + 1)
                       .select('CustomerID', 'Importe Total'))

            df_view = df_view.union(df_nottop10)

            return df_view
            
    #METODO 3      
    # Retention rate = nº of clients retained / total clients in that period
    # Churn rate = nº of clients lost / total clients in the period
    # One month by default
    def get_porcentaje_de_retencion_de_clientes(self, time_period=2592000):
        result = 0
        if(self is not None):
            # Consider if we need to evaluate how long it has been since a
            # client bought something, and how much was the time between
            # interactions

            # window = Window.partitionBy("Cliente").orderBy("Fecha")

            # Clients who bought more than one time within the last... 30 days by default
            # Modified to not use udf's and to take into account the date
            # The point in time to analyze the data is now by default
            time_from = current_timestamp().astype('bigint')

            # Calculate the number of interactions of clients within the time
            # period
            base = self
                .select('CustomerID', 'InvoiceDate')
                .where((time_from - unix_timestamp(col('InvoiceDate'), 'dd/MM/yyyy HH:mm')) < time_period)

            # Those that interact more than 1 time are considered retained
            retained = base.groupBy('CustomerID').agg(count('InvoiceDate').alias(
                'Interacciones')).where(col('Interacciones') >= 2).count()

            # Total individual clients in the dataset within the same period
            # Change to shared acumulator
            total = 0
            total = base.agg(countDistinct('CustomerID')).collect()[0][0]

            if(total == 0):
                total = 1

            result = (float(retained) / float(total)) * 100

        return result
    #Metodo 4
    def get_valor_medio_de_compra_por_cliente(self, data_step_size_rounded=10):
        df = None
        if data_step_size_rounded is None:
            data_step_size_rounded = 10
        #Que hago con get_invoice_stats_by_client()
        if(self is not None):
            config = {}
            config['target_column'] = 'avg(total)'
            config['data_step_size_rounded'] = data_step_size_rounded
            df = (self
                    .withColumn(config['target_column'], floor(col(config['target_column']) / config['data_step_size_rounded']) * config['data_step_size_rounded'])
                    .groupBy(config['target_column'])
                    .agg(count(config['target_column']).alias('Clientes'))
                    .sort(col(config['target_column']))
                  )
        return df
    
    #METODO 5
    def get_distribucion_de_clientes_por_facturas_emitidas(self):
        df = None
        if(self is not None):
            df = (self
                    #InvoiceID equivale a InvoiceNo
                    #.agg(count('_invoice_id').alias('facturas'), sum('Total').alias('Total'))
                    .select('CustomerID', 'InvoiceNo', 'Total')
                    .groupBy('CustomerID')
                    .agg(count('InvoiceNo').alias('facturas'), sum('Total').alias('Total'))
                    .withColumn('facturas', floor(col('facturas') / 10) * 10)
                    .groupBy('facturas')
                    .agg(count('CustomerID').alias('Clientes'))
                    .sort('facturas'))
            return df
        
    #METODO 6
    def get_distribucion_de_beneficios_por_facturas_emitidas(self):
        df = None
        if(self is not None):
            df = (self
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
    
