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
