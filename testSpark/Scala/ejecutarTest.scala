%spark

var iteraciones = 1
var i = 0
var inicial, fin, total, tiempo, media, tota:Double = 0//0 0.asInstanceOf[Long]
var datas = get_invoice_lines_positive()
println("INICIO DE LAS PRUEBAS DE RENDIMIENTO CON SCALA")
println("MODULO 1 : GET_TOP_CLIENTES_POR_IMPORTE_TOTAL")

for(i <- 1 to iteraciones )
{
    inicial = System.currentTimeMillis()
    get_top_clientes_por_importe_total(datas,0)
    tiempo = (System.currentTimeMillis() - inicial )/ 1e3d
    total = total + tiempo
    println("Tiempo Iteracion " + i + ": "+ tiempo)
}
media = total / iteraciones
println("El tiempo del modulo 1 es: ")
println(media)
println("MODULO 2 : GET_DISTRIBUCION_DE_CLIENTES_POR_IMPORTE")
total=0
for(i <- 1 to iteraciones )
{
    inicial = System.currentTimeMillis()
    get_distribucion_de_clientes_por_importe(datas,0)
    tiempo = (System.currentTimeMillis() - inicial )/ 1e3d
    total = total + tiempo
    println("Tiempo Iteracion " + i + ": "+ tiempo)
}
media = total / iteraciones
println("El tiempo del modulo 2 es: ")
println(media)
println("MODULO 3 : GET_PORCENTAJE_DE_RETENCION_DE_CLIENTES")
total = 0
media = 0
for(i <- 1 to iteraciones )
{
    inicial = System.currentTimeMillis()
    get_porcentaje_de_retencion_de_clientes(datas)
    tiempo = (System.currentTimeMillis() - inicial )/ 1e3d
    tota = tota + tiempo
    println("Tiempo Iteracion " + i + ": "+ tiempo)
}
media = tota / iteraciones
println("El tiempo del modulo 3 es: ")
println(media)
/*
println("MODULO 4 : GET_PORCENTAJE_DE_RETENCION_DE_CLIENTES")
total = 0
for(i <- 0 to iteraciones )
{
    inicial = System.currentTimeMillis()
    get_porcentaje_de_retencion_de_clientes(datas)
    tiempo = (System.currentTimeMillis() - inicial )/ 1e3d
    total = total + tiempo
}
media = total / iteraciones
println("El tiempo del modulo 4 es: ")
println(media)*/
println("MODULO 5 : GET_DISTRIBUCION_DE_CLIENTES_POR_FACTURAS_EMITIDAS")
total = 0
for(i <- 1 to iteraciones )
{
    inicial = System.currentTimeMillis()
    get_distribucion_de_clientes_por_facturas_emitidas(datas)
    tiempo = (System.currentTimeMillis() - inicial )/ 1e3d
    total = total + tiempo
    println("Tiempo Iteracion " + i + ": "+ tiempo)
}
media = total / iteraciones
println("El tiempo del modulo 5 es: ")
println(media)
println("MODULO 6 : GET_DISTRIBUCION_DE_BENEFICIOS_POR_FACTURAS_EMITIDAS")
total = 0
for(i <- 1 to iteraciones )
{
    inicial = System.currentTimeMillis()
    get_distribucion_de_beneficios_por_facturas_emitidas(datas)
    tiempo = (System.currentTimeMillis() - inicial )/ 1e3d
    total = total + tiempo
    println("Tiempo Iteracion " + i + ": "+ tiempo)
}
media = total / iteraciones
println("El tiempo del modulo 6 de media es: ")
println(media)
println("LECTURA DE CSV : sqlContext.read.csv(billing.csv)")
total = 0
for(i <- 1 to iteraciones )
{
    inicial = System.currentTimeMillis()
    var test = get_invoice_lines_positive2()
    tiempo = (System.currentTimeMillis() - inicial )/ 1e3d
    total = total + tiempo
    println("Tiempo Iteracion " + i + ": "+ tiempo)
}
media = total / iteraciones
println("El tiempo LECTURA DE CSV : sqlContext.read.csv(billing.csv): ")
println(media)
println("LECTURA DE CSV: spark.read.format(csv)")
total = 0
for(i <- 1 to iteraciones )
{
    inicial = System.currentTimeMillis()
    var test = get_invoice_lines_positive3()
    tiempo = (System.currentTimeMillis() - inicial )/ 1e3d
    total = total + tiempo
    println("Tiempo Iteracion " + i + ": "+ tiempo)
}
media = total / iteraciones
println("El tiempo LECTURA DE CSV: spark.read.format(csv): ")
println(media)
println("LECTURA DE CSV : sqlContext.read.format(com.databricks.spark.csv)")
total = 0
for(i <- 1 to iteraciones )
{
    inicial = System.currentTimeMillis()
    var test = get_invoice_lines_positive4()
    tiempo = (System.currentTimeMillis() - inicial )/ 1e3d
    total = total + tiempo
    println("Tiempo Iteracion " + i + ": "+ tiempo)
}
media = total / iteraciones
println("tiempo LECTURA DE CSV : sqlContext.read.format(com.databricks.spark.csv")
println(media)
