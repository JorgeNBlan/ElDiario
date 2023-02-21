import pandas as pd

'''
-----------
+++++++++++
EJERCICIO 1
+++++++++++
-----------
'''

#Voy a definir una lista de dataframes 
df_list = []

#------
#PANDAS
#------


def run_function_pandas():
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
    global df_list
    df_list = [df_usuario, df_suscripcion]
    return df_list
    


'''
-----------------------
+++++++++++++++++++++++
EJERCICIO 2 - EN PANDAS
+++++++++++++++++++++++
-----------------------
'''


def proceso_agrupacion_tablas_pandas(obj_list):
    #Filtrado del dataframe de suscritpres para quedarnos con las suscripciones activas
    pB = obj_list[1][obj_list[1]['suscripcion_activa']==True]
    #Cruce con el dataframe de usuario para devolver la fecha de creacion SIN el tipo de usuario.
    #Se le deberá añadir información a la tabla de suscripcion
    pA = obj_list[0].drop('tipo_usuario', axis = 1)
    pC = pd.merge(pA,pB,on='user_id',how='right')
    #Ordenar el dataframe por las suscripciones más antiguas, no hace falta indicar ascending, ya que por defecto es True
    pC.sort_values(by=['fecha_creacion_suscripcion'], inplace = True)
    #Realizar un conteo y mostrado por pantalla de las suscripciones
    print(pC['suscription_id'].count())
    return pC
    

