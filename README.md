# ElDiario
Practica

------------
EJERCICIO 5:
------------
Primero decir que nunca se me ha dado dicha circunstancia pero se me ocurren varias opciones:
-Antes de subir a bigQuery siempre creo un datalake (bucket de Google Cloud Storage) y subo los archivos usando:
client = storage.Client()
bucket = client.get_bucket('my_bucket')
blob = bucket.blob('file.csv')
blob = upload_from_filename('file.csv')

Desde BiGQuery conecto directamente con Google Cloud Storage

-Si aún así el proceso falla dividiría el .csv en diferentes partes paginando para intentar reconstruir en destino. (subirlo por trozos)

-Si el problema está en el rendimiento de la propia máquina de spark, lo transformaría a un RDD e intentaría aumtentar el cluster añadiendo más máquinas o quizás intentar procesar todo directamente desde GCP...


------------
EJERCICIO 6:
------------

No tengo muy claro  a que se refiere con "cachear" los datos, supongo que es usar RDD (dataset distribuido en el cluster), en este contexto se probaría la solución cuando el tiempo de ejecución usando dataframe o spark.dataframe sea completamente inviable.
