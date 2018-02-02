
data = APP.get_datos()
iteraciones = 30
print("INICIO DE LAS PRUEBAS DE RENDIMIENTO EN PYSPARK!!!!!")
print("MODULO 1: GET_TOP_CLIENTES_POR_IMPORTE_TOTAL")
tiempo = 0
for x in range(0,iteraciones):
    inicial = time.time()
    test.get_top_clientes_por_importe_total(data)
    fin = time.time()
    total = fin - inicial
    tiempo = tiempo + total
    print("Tiempo iteracion " +  str(x) + " : " + str(total))
media = tiempo/iteraciones 
print("El tiempo de media en el modulo 1 es: " )
print(media)

print("MODULO 2: GET_DISTRIBUCION_DE_CLIENTES_POR_IMPORTE")
tiempo = 0
for x in range(0,iteraciones):
    inicial = time.time()
    test.get_distribucion_de_clientes_por_importe(data)
    fin = time.time()
    total = fin - inicial
    tiempo = tiempo + total
    print("Tiempo iteracion " +  str(x) + " : " + str(total))
media = tiempo/iteraciones
print("El tiempo de media en el modulo 2 es: " )
print(media)
print("MODULO 3: GET_PORCENTAJE_DE_RETENCION_DE_CLIENTES")
tiempo = 0
for x in range(0,iteraciones):
    inicial = time.time()
    test.get_porcentaje_de_retencion_de_clientes(data)
    fin = time.time()
    total = fin - inicial
    tiempo = tiempo + total
    print("Tiempo iteracion " +  str(x) + " : " + str(total))
media = tiempo/iteraciones
print("El tiempo de media en el modulo 3 es: " )
print(media)

print("MODULO 5: GET_DISTRIBUCION_DE_CLIENTES_POR_FACTURAS_EMITIDAS")
tiempo = 0
for x in range(0,iteraciones):
    inicial = time.time()
    test.get_distribucion_de_clientes_por_facturas_emitidas(data)
    fin = time.time()
    total = fin - inicial
    tiempo = tiempo + total
    print("Tiempo iteracion " +  str(x) + " : " + str(total))
media = tiempo/iteraciones
print("El tiempo de media en el modulo 5 es: " )
print(media)

print("MODULO 6: GET_DISTRIBUCION_DE_BENEFICIOS_POR_FACTURAS_EMITIDAS")
tiempo = 0
for x in range(0,iteraciones):
    inicial = time.time()
    test.get_distribucion_de_beneficios_por_facturas_emitidas(data)
    fin = time.time()
    total = fin - inicial
    tiempo = tiempo + total
    print("Tiempo iteracion " +  str(x) + " : " + str(total))
media = tiempo/iteraciones
print("El tiempo de media en el modulo 6 es:  " )
print(media)


