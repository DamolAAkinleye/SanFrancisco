%spark
//test Scala

//Metodo 1
def get_top_clientes_por_importe_total(data: DataFrame, top_count: Int): DataFrame = 
{
        //Inicializacion de variables
        var top_count_aux= top_count
        var df = sqlContext.emptyDataFrame
        var decimal_to_euro= udf((num:Int) => num%.2f)
        if(top_count == 0)
        {
            top_count_aux = 10
        }
	//Operacion SQL
        if(data != None)
        {
            df = (data
                  .groupBy("CustomerID")
                  .agg(sum("Total").alias("Importe Total"))
                  .sort(desc("Importe Total"))
                  .limit(top_count_aux)
                  )
        }
        return df
}
// MODULO 2
def get_distribucion_de_clientes_por_importe(data: DataFrame, top_count:Int): DataFrame =
{
        var decimal_to_euro= udf((num:Int) => num%.2f)
        var top_count_aux = top_count
        var clientes = sqlContext.emptyDataFrame

        if(top_count == 0)
        {
            top_count_aux = 10
        }

        if(data != None)
        {
            clientes = data
                          .groupBy("CustomerID")
                          .agg(sum("Total").alias("Importe Total"))
                          .sort(desc("Importe Total"))
                          .cache()
        }
        
        var clientes_top10 = clientes.limit(top_count_aux)
        var nottop10_total = clientes.agg(sum("Importe Total")).head().getDouble(0) - clientes_top10.agg(sum("Importe Total")).head().getDouble(0)
        var df_nottop10_data = List(("Resto de clientes", nottop10_total))
        var df_nottop10 = df_nottop10_data.toDF("CustomerID","Importe Total")


        var df_view = (clientes_top10
                       .select("CustomerID", "Importe Total"))
        df_view = df_view.union(df_nottop10)

        return df_view
}

/*Metodo 3
Ratio de retencion = nº de clientes / total
*/
    def get_porcentaje_de_retencion_de_clientes(data:DataFrame): Float =
    {
        val format = ""
        val time_period = 2592000
        var result = 0f
        if(data != None)
        {
            var time_from = current_timestamp().cast("long")
            val base = get_invoice_lines_positive().select("CustomerID", "InvoiceDate")
                .where((time_from - unix_timestamp(col("InvoiceDate"), "dd/MM/yyyy HH:mm")) < time_period)

            val retained = base.groupBy("CustomerID").agg(count("InvoiceDate").alias(
                "Interacciones")).where(col("Interacciones") >= 2).count()
            var total = 0f
            if(total == 0)
                total = 1

            result = (retained.asInstanceOf[Float] / total) * 100
        }   
        return result
    }
//Modulo 5
def get_distribucion_de_clientes_por_facturas_emitidas(data : DataFrame): DataFrame =
{
    var df = sqlContext.emptyDataFrame
    if(data != None)
    {
        df = (data
                //InvoiceID equivale a InvoiceNo
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
def get_distribucion_de_beneficios_por_facturas_emitidas(data:DataFrame): DataFrame =
{
    var df = sqlContext.emptyDataFrame
    if(self != None)
    {
        df = (data
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
