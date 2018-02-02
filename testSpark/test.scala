%spark
//Codigo Clientes
//var self = sqlContext.emptyDataFrame

def debug(self: DataFrame): Unit = 
{
        println("%html <h1>Class Clientes</h1>")
        println("%html <h2>Clientes.debug()</h2>")
        println("%html <h3>Clientes.debug() -> self.dataframes</h3><p>%s</p>" + 
              "<br>")//.join(self.count))
        println("%html <h3>Clientes.debug() -> Dataframes</h3>")
        //dataframe.count() obtiene el tamaño de un dataframe, es equivalente a dataframes.items()????
        var tam = self.count()
        tam = tam.toInt
        for (key <- self)
        {
            println("%html <h4>Dataframe:" + key +"</h4> <pre>")
        }
}

def get_top_clientes_por_importe_total(self: DataFrame, top_count: Int): DataFrame = 
{
        //Inicializacion de variables
        var top_count_aux= top_count
        var df = sqlContext.emptyDataFrame
       // decimal_to_euro = udf(
         //   lambda num: "{0:,.2f} €".format(num), StringType())
        var decimal_to_euro= udf((num:Int) => num%.2f)
        if(top_count == 0)
        {
            top_count_aux = 10
        }
        if(self != None)
        {
            df = (self
                  .groupBy("CustomerID")
                  .agg(sum("Total").alias("Importe Total"))
                  .sort(desc("Importe Total"))
                  .limit(top_count_aux)
                  //.withColumn('Posicion', monotonically_increasing_id() + 1) # Omitido para facilitar la representacion
                  )
        }
        return df
}
// MODULO 2
def get_distribucion_de_clientes_por_importe(self: DataFrame, top_count:Int): DataFrame =
{
        var decimal_to_euro= udf((num:Int) => num%.2f)
        //var df_view = sqlContext.emptyDataFrame
        var top_count_aux = top_count
        var gb_clients = sqlContext.emptyDataFrame

        if(top_count == 0)
        {
            top_count_aux = 10
        }

        if(self != None)
        {
            gb_clients = self
                          .groupBy("CustomerID")
                          .agg(sum("Total").alias("Importe Total"))
                          .sort(desc("Importe Total"))
                          .cache()
        }
        
        var gb_clients_top10 = gb_clients.limit(top_count_aux)
        // perhaps calculating totals instead of operating over rows it is faster?
        //.head().getInt(row) get the first value of dataframe that it have Int as type, in this case, we choose Double.
        var nottop10_total = gb_clients.agg(sum("Importe Total")).head().getDouble(0) - gb_clients_top10.agg(sum("Importe Total")).head().getDouble(0)
        var df_nottop10_data = List(("Resto de clientes", nottop10_total))
        var df_nottop10 = df_nottop10_data.toDF("CustomerID","Importe Total")
            //var df_nottop10 = sqlContext.createDataFrame(df_nottop10_data, gb_clients_top10.schema)

        var df_view = (gb_clients_top10//.withColumn('Posicion', monotonically_increasing_id() + 1)
                       .select("CustomerID", "Importe Total"))
        df_view = df_view.union(df_nottop10)

        return df_view
}

/*Metodo 3
 # Retention rate = nº of clients retained / total clients in that period
    # Churn rate = nº of clients lost / total clients in the period
    # One month by default*/
    def get_porcentaje_de_retencion_de_clientes(self:DataFrame): Float =
    {
        val format = ""
        val time_period = 2592000
        var result = 0f
        if(self != None)
        {
            /*# Consider if we need to evaluate how long it has been since a
            # client bought something, and how much was the time between
            # interactions

            # window = Window.partitionBy("Cliente").orderBy("Fecha")

            # Clients who bought more than one time within the last... 30 days by default
            # Modified to not use udf's and to take into account the date
            # The point in time to analyze the data is now by default*/
            var time_from = current_timestamp().cast("long")
            //time_from = time_from.withColumn(col("current_timestamp").cast(BigInt))
            /*# Calculate the number of interactions of clients within the time
            # period*/
            val base = get_invoice_lines_positive().select("CustomerID", "InvoiceDate")
                .where((time_from - unix_timestamp(col("InvoiceDate"), "dd/MM/yyyy HH:mm")) < time_period)

            //# Those that interact more than 1 time are considered retained
            val retained = base.groupBy("CustomerID").agg(count("InvoiceDate").alias(
                "Interacciones")).where(col("Interacciones") >= 2).count()

            /*# Total individual clients in the dataset within the same period
            # Change to shared acumulator*/
            var total = 0f
            //total = base.agg(countDistinct("CustomerID")).head().getFloat(0)
            if(total == 0)
                total = 1

            result = (retained.asInstanceOf[Float] / total) * 100
        }   
        return result
    }
//Modulo 4
/*def get_valor_medio_de_compra_por_cliente(self:DataFrame, data_step_size_rounded:Int): Int = 0
{
    
        var df = sqlContext.emptyDataFrame
        var data_step_size = data_step_size_rounded
        if(data_step_size_rounded == 0)
            data_step_size = 10
        
        Que hago con get_invoice_stats_by_client()
        if(self.app.get_invoice_stats_by_client() is not None):
            config = {}
            config['target_column'] = 'avg(total)'
            config['data_step_size_rounded'] = data_step_size_rounded
            df = (self.app.get_invoice_stats_by_client()
                  .withColumn(config['target_column'], floor(col(config['target_column']) / config['data_step_size_rounded']) * config['data_step_size_rounded'])
                  .groupBy(config['target_column'])
                  .agg(count(config['target_column']).alias('Clientes'))
                  .sort(col(config['target_column']))
                  )
        return df
}*/
//Modulo 5
def get_distribucion_de_clientes_por_facturas_emitidas(self : DataFrame): DataFrame =
{
    var df = sqlContext.emptyDataFrame
    if(self != None)
    {
        df = (self
                //InvoiceID equivale a InvoiceNo
                //.agg(count('_invoice_id').alias('facturas'), sum('Total').alias('Total'))
                .select("CustomerID", "InvoiceNo", "Total")
                .groupBy("CustomerID")
                .agg(count("InvoiceNo").alias("facturas"), sum("Total").alias("Total"))
                .withColumn("facturas", floor(col("facturas") / 10) * 10)
                .groupBy("facturas")
                .agg(count("CustomerID").alias("Clientes"))
                .sort("facturas"))
                return df
    }
    return df
}
//modulo 6
def get_distribucion_de_beneficios_por_facturas_emitidas(self:DataFrame): DataFrame =
{
    var df = sqlContext.emptyDataFrame
    if(self != None)
    {
        df = (self
                  .select("CustomerID", "InvoiceNo", "Total")
                  .groupBy("CustomerID")
                  .agg(count("InvoiceNo").alias("facturas"),
                       sum("Total").alias("Total"))
                  .withColumn("facturas", floor(col("facturas") / 10) * 10)
                  .groupBy("facturas")
                  .agg(sum("Total").alias("Importe Total"))
                  .sort("facturas"))
    }
    return df
}
