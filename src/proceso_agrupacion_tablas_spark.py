'''
Para arrancar mi cluster local:
export PYSPARK_PYTHON=python3
export SPARK_HOME=/home/server1/spark-3.0.1-bin-hadoop3.2
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

NOTA:En mi caso no creo una sesión de spark como se indica en el ejercicio, ejecuto mi cluster
Al difinir las variables de entorno citadas arriba ejecuto directamente la shell de pyspark
Desde la shell de pyspark puedo ejecutar los ejercicios en paralelo por simplificar
'''
from collections.abc import Callable
from pyspark.sql import SparkSession
import pandas as pd

#Voy a definir una lista de spark.dataframes
spdf_list = []


'''
-----------
+++++++++++
EJERCICIO 1
+++++++++++
-----------
'''

#-------
#PYSPARK
#-------

#def run_function(spark):
def run_function_spark():
    dict_tablon_usuario = {"user_id": [1, 2, 3, 4],
                            "fecha_creacion": ["2020-02-01", "2020-02-01","2020-02-01", "2020-02-01"],
                            "tipo_usuario": ["Socio", "Registrado", "Socio",
                            "Socio"]}
                            
    dict_tablon_suscripcion = {"suscription_id": [1, 2, 3, 4],
                                "user_id": [1, 3, 1, 4],
                                "fecha_creacion_suscripcion": ["2020-01-01",
                                "2022-12-01", "2022-12-01", "2020-01-01"],
                                "suscripcion_activa": [False, True, True, True]}
    df_usuario = pd.DataFrame(dict_tablon_usuario)
    df_suscripcion = pd.DataFrame(dict_tablon_suscripcion)
    #Creo los sparkDataFrames desde los propios dataframes que ya tienen esquema
    #Seguramente existe otra manera de crear los SparkDatraFrames desde el diccionario pero yo no la conozco
    spdf_usuario = spark.createDataFrame(data=df_usuario)
    spdf_suscripcion = spark.createDataFrame(data=df_suscripcion)
    global spdf_list
    spdf_list = [spdf_usuario, spdf_suscripcion]
    return spdf_list


'''
-----------------------
+++++++++++++++++++++++
EJERCICIO 3 - EN PYSPARK
+++++++++++++++++++++++
-----------------------
'''

'''
NOTA: Como ya he comntado en mi entorno no es necesario realizar la sesión

def create_spark_session():
    builder = SparkSession.builder
    
    spark = builder.getOrCreate()
return spark

def launcher(func: Callable):
    spark = create_spark_session()
    
    func(spark)

'''


def proceso_agrupacion_tablas_spark(spobj_list):
    #Filtrado del dataframe de suscritpres para quedarnos con las suscripciones activas
    #B =  spobj_list[1].filter(spobj_list[1].suscripcion_activa == True), se puede utilizar también
    sB =  spobj_list[1].filter(spobj_list[1]['suscripcion_activa'] == True)
    #Cruce con el dataframe de usuario para devolver la fecha de creacion SIN el tipo de usuario.
    #Se le deberá añadir información a la tabla de suscripcion
    sA = spobj_list[0].drop('tipo_usuario')
    sC = sB.join(sA, sB['user_id'] == sA['user_id'], 'inner')
    #Ordenar el dataframe por las suscripciones más antiguas, no hace falta poner ascending = True ya que está por defecto
    sC = sC.orderBy(['fecha_creacion_suscripcion'])
    #Realizar un conteo y mostrado por pantalla de las suscripciones
    # se puede también importar from pyspark.sql.functions import count y ejecutar C.select(count(C.suscription_id)).show() (por analogía con el dataframe) pero se puede hacer directamente contando registros con C.count() sin importar nada
    print(sC.count())
    return sC.show()


