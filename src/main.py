from proceso_agrupacion_tablas_pandas import *
from proceso_agrupacion_tablas_spark import *



#------
#PANDAS
#------
proceso_agrupacion_tablas_pandas(run_function_pandas())

#-----
#SPARK
#-----

#En este caso la ejecución no funcionará, ya que se requiere crear una sessión directa a pyspark
#En mi caso, siempre que he ejecutado pyspark lo he realizado directamente desde su consola como he indicado en el ejercicio.

proceso_agrupacion_tablas_spark(run_function_spark())