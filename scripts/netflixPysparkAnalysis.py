# %% [markdown]
# # <center><font color='#DE2138'>Carlos Vega González: NetflixAnalysis.ipynb</font></center>
# ---
# 
# ## Índice de contenidos
# 
# ### [1. Fase de carga de datos](#fase-carga)  
# [1.1 Importación de librerías](#librerias)  
# [1.2 Carga de datos](#datos)  
# 
# ---
# ### [2. Fase de limpieza de datos](#fase-limpieza)  
# [2.1 Renombrado de columnas](#columnas)  
# [2.2 Conteo de nulos](#nulos)  
# [2.3 Limpieza de categoría e ID](#categoria-id)  
# [2.4 Eliminación de valores inválidos](#valores-invalidos)  
# [2.5 Estandarización de duración](#duracion)  
# [2.6 Validación de fechas en Netflix](#fechas-netflix)  
# [2.7 Validación del año de estreno](#ano-estreno)  
# [2.8 Normalización de subcategoría](#subcategoria)  
# [2.9 Limpieza de género](#genero)  
# [2.10 Conversión de tipos de datos](#conversion)  
# 
# ---
# ### [3. Fase de exploración de datos](#fase-exploracion)  
# [3.1 Duración promedio por país](#duracion-pais)  
# [3.2 Actor más frecuente en musicales](#actor-musicales)  
# [3.3 Diferencia semanal entre películas ](#diferencia)  
# [3.4 Transformación de género a array](#genero-array)  
# [3.5 Conteo por número de países](#conteo-paises)  
# [3.6 Exportación a Parquet](#parquet)  
# 
# ---

# %% [markdown]
# 
# 
# ---
# 
# 
# ## **FASE DE CARGA DE DATOS**
# 
# ---
# 
# 

# %% [markdown]
# ## <h2 id="carga"><center><font color='#1E90FF'>Limpieza y Análisis de Datos de Netflix</font></center></h2>
# 
# En este documento se detalla el proceso completo de limpieza y análisis de un dataset de títulos de Netflix. Para ello se describen paso a paso las transformaciones realizadas, con justificaciones técnicas de las decisiones tomadas. Cada sección incluye una explicación en Markdown previa al código que se entiende como suficiente para este cuaderno. Por esta misma razón en el código no hay casi comentarios, porque la explicación se cubre en las celdas previas

# %% [markdown]
# ##### Justificación del Uso de CamelCase en Variables y Funciones
# 
# A lo largo del desarrollo de este notebook, se ha utilizado la convención **CamelCase** para los nombres de variables y funciones, en lugar de la convención tradicional de Python `snake_case`. Este enfoque no ha sido casual, sino que responde a razones de práctica. Aunque en algunos puntos puede haberse colado el uso de `snake_case`, el objetivo ha sido mantener una coherencia con el estilo de Apache Spark y su ecosistema para practicar todo tipo de sintaxis.

# %% [markdown]
# ---
# 
# ## <h2 id="carga"><center><font color='#1E90FF'>Importación de Librerías</font></center></h2><a id="librerias"></a>
# 
# Antes de comenzar con el análisis, preparé el entorno importando todas las librerías necesarias y asegurando una correcta configuración. Debido a que utilizaremos PySpark para el procesamiento avanzado de datos, primero hago la comprobación de que esté correctamente instalado, procediendo con su instalación automática en caso de no encontrarse disponible.
# 
# Además, importé de manera explícita funciones específicas desde pyspark.sql.functions, evitando la práctica poco recomendada del import *. Para facilitar futuras tareas de extracción automática de información desde páginas web (fechas de estreno desde IMDb), añadí las librerías de scraping requests y BeautifulSoup desde esta etapa inicial del proyecto.

# %%
try:
    import pyspark
except ImportError:
    !pip install pyspark

#Todas las librerías para evitar F.
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, trim, split, when, to_date, length, sum, avg, round, floor,
    expr, lower, explode, max, min, datediff, count, udf
)
from pyspark.sql.types import IntegerType

#Para scraping
import requests
from bs4 import BeautifulSoup

# %% [markdown]
# ## <h2 id="carga"><center><font color='#1E90FF'>Carga de datos</font></center></h2><a id="datos"></a>
# 
# En este paso, inicié una sesión de Spark y cargué los datos del conjunto de Netflix. Debido a que los datos originales estaban divididos en múltiples archivos CSV comprimidos (siete archivos numerados), utilicé un patrón con wildcards (*) para facilitar la lectura simultánea de todos los archivos en una única instrucción con Spark.
# 
# Específicamente, utilicé la ruta /tmp/netflix_titles_dirty_*.csv para combinar todos los archivos coincidentes en un único DataFrame. Además, configuré la carga especificando el separador como tabulación (sep="\t") y permitiendo reconocer el esquema automáticamente.
# 
# Tras la carga inicial en el DataFrame df, realicé una inspección rápida del esquema y los datos cargados. Este chequeo inicial reveló columnas con nombres genéricos (por ejemplo, _c0, _c1), señalando la necesidad de asignar nombres más significativos para optimizar futuras operaciones.
# 
# ### ¿Cómo lo resolví?
# Durante esta fase del proceso, realicé las siguientes acciones clave para preparar los datos:
# 
# 1. Creé una sesión de Spark mediante la clase SparkSession.
# 2. Leí múltiples archivos simultáneamente aprovechando rutas con wildcard (*.csv).
# 3. Especificé explícitamente el separador de columnas para asegurar una correcta interpretación de los datos.
# 4. Activé la inspección inicial del DataFrame para identificar columnas con nombres poco descriptivos.
# 
# ### Características del código
# - **Uso eficiente de rutas wildcard:** Aproveché patrones para combinar múltiples archivos fácilmente.
# - **Verificación preliminar del esquema`** Utilicé inspecciones rápidas para identificar mejoras inmediatas en la estructura.
# 

# %%
# Intenté hacerlo a varias líneas pero había que hacer el bucle aplanado
# !for i in {01..07}; do
#     wget -O "/tmp/netflix_titles_dirty_$i.csv.gz" "https://github.com/datacamp/data-cleaning-with-pyspark-live-training/blob/master/data/netflix_titles_dirty_$i.csv.gz?raw=True"
# done


# %%
!for i in {01..07}; do wget -O "/tmp/netflix_titles_dirty_$i.csv.gz" "https://github.com/datacamp/data-cleaning-with-pyspark-live-training/blob/master/data/netflix_titles_dirty_$i.csv.gz?raw=True"; done

!for file in /tmp/netflix_titles_dirty_*.csv.gz; do gunzip -c "$file" > "${file%.gz}"; done

!ls -lh /tmp/netflix_titles_dirty_* # Added ! to execute as shell command

# %%
spark = SparkSession.builder.appName("netflixAnalysis").getOrCreate()

file_path = "/tmp/netflix_titles_dirty_*.csv"
df = spark.read.csv(file_path, sep="\t", inferSchema=True)

print("### DataFrame inicial ###")
print(f"Filas totales (antes de cualquier limpieza): {df.count()}")
df.show(3)
df.printSchema()

# %% [markdown]
# 
# 
# ---
# 
# 
# ## **FASE DE LIMPIEZA DE DATOS**
# 
# ---
# 
# 

# %% [markdown]
# ## <center><font color='#1E90FF'>Renombrado de columnas  <a id="columnas"></a></font></center>
# 
# El siguiente paso consistió en **renombrar las columnas del DataFrame**, que originalmente presentaban nombres genéricos (ej. `_c0`, `_c1`, `_c2`), por nombres más descriptivos que facilitan enormemente la comprensión del análisis posterior.
# 
# Para ello, creé un diccionario donde almacené los pares clave-valor con el formato original y el nuevo nombre deseado:
# 
# - `_c0` → **id**: Identificador único del título.
# - `_c1` → **categoria**: Tipo de contenido ("Movie" o "TV Show").
# - `_c2` → **titulo**: Título del contenido.
# - `_c3` → **director**: Nombre del director o elenco de directores/as.
# - `_c4` → **reparto**: Actores principales del contenido.
# - `_c5` → **pais**: País de origen del contenido.
# - `_c6` → **estrenoNetflix**: Fecha de estreno en Netflix.
# - `_c7` → **anioEstrenoReal**: Año original de lanzamiento del contenido.
# - `_c8` → **subcategoria**: Clasificación de edad o subcategoría adicional.
# - `_c9` → **duracion**: Duración en minutos o número de temporadas.
# - `_c10` → **genero**: Género del contenido.
# - `_c11` → **descripcion**: Descripción o sinopsis del contenido.
# 
# ### ¿Cómo lo resolví?
# 
# Me enfoqué en estos puntos clave para clarificar el contenido del DataFrame:
# 
# - **Definí claramente un diccionario** con los nuevos nombres de columnas.
# - Utilicé la función `withColumnRenamed()` de Spark para actualizar los nombres fácilmente.
# - Realicé una comprobación rápida con `.show()` para asegurar que los cambios fueran efectivos y las columnas reflejaran correctamente la información.

# %%
df = df.withColumnRenamed("_c0", "id")\
       .withColumnRenamed("_c1", "categoria")\
       .withColumnRenamed("_c2", "titulo")\
       .withColumnRenamed("_c3", "director")\
       .withColumnRenamed("_c4", "reparto")\
       .withColumnRenamed("_c5", "pais")\
       .withColumnRenamed("_c6", "estrenoNetflix")\
       .withColumnRenamed("_c7", "anioEstrenoReal")\
       .withColumnRenamed("_c8", "subcategoria")\
       .withColumnRenamed("_c9", "duracion")\
       .withColumnRenamed("_c10", "genero")\
       .withColumnRenamed("_c11", "descripcion")

df.show(3)

# %% [markdown]
# Tras la primera revisión manual de los datos, lo primero que se asume es que puede haber registros en los que director y reparto hayan sido intercambiados accidentalmente, pero es imposible diferenciarlos de manera automática debido a la falta de un patrón claro para detectar estos errores (en ambos hay nomnbres de una o varias personas)

# %% [markdown]
# ## <center><font color='#1E90FF'>Conteo de nulos <a id="nulos"></a></font></center>
# 
# A continuación, realicé una evaluación preliminar del conjunto de datos original, enfocándome especialmente en identificar la cantidad de valores nulos presentes en cada columna. Este análisis inicial resulta crucial para determinar la magnitud del problema de datos incompletos.
# 
# Para llevar a cabo esta tarea, implementé la función personalizada `contar_nulos()`, basada en funciones nativas de PySpark. Esta función permitió obtener rápidamente un resumen numérico sobre los datos faltantes, facilitando decisiones posteriores en la limpieza.
# 
# ### ¿Cómo lo resolví?
# 
# En esta fase, mi enfoque fue obtener un panorama claro sobre la calidad inicial de los datos, especialmente respecto a valores nulos. Los pasos seguidos fueron:
# 
# - Creé la función `contar_nulos()` para identificar rápidamente columnas problemáticas.
# - Ejecuté esta función sobre el DataFrame original antes de cualquier modificación.
# - Analicé la salida para identificar prioridades en los siguientes pasos del proceso de limpieza.
# 

# %%
def contar_nulos(df):
    nulos_df = df.select([(sum(col(c).isNull().cast("int")).alias(c)) for c in df.columns])

    resultado = nulos_df.toPandas().transpose().reset_index().rename(columns={'index': 'columna', 0: 'nulos'})
    return resultado

resultado = contar_nulos(df)
print(resultado)


# %% [markdown]
# ## <center><font color='#1E90FF'>Limpieza de categoría e ID <a id="categoria-id"></a></font></center>
# 
# El siguiente paso consistió en limpiar y estandarizar las columnas `categoria` e `id`, cruciales para identificar los registros de forma única y clasificar el contenido adecuadamente. Para lograrlo, ejecuté estos pasos:
# 
# - **Filtrado de categorías:** Conservé únicamente las filas con categorías válidas (`Movie`, `TV Show`), eliminando cualquier categoría inesperada.
# - **Conversión de ID:** Transformé los identificadores (`id`) al tipo numérico entero (`IntegerType`). Esto marcó como nulos los IDs no numéricos.
# - **Eliminación de IDs inválidos:** Descarté registros cuyo ID convertido fuese nulo o no cumpliera exactamente con la longitud requerida de 8 dígitos (asumiendo que los IDs de Netflix siguen este estándar).
# - **Eliminación de duplicados:** Finalmente, removí registros duplicados basados en la columna `id` para asegurar la unicidad.
# 
# Encapsulé todo el procedimiento en la función `limpiarCategoriaId()`, que posteriormente apliqué sobre el DataFrame original `df`, obteniendo así un nuevo DataFrame limpio denominado `dfLimpio`. Este nuevo conjunto de datos contiene exclusivamente películas y series correctamente identificadas por IDs únicos y válidos.
# 
# ---
# 

# %%
def limpiarCategoriaId(df):
    categoriasValidas = ["Movie", "TV Show"]
    dfLimpio = df.filter(df.categoria.isin(categoriasValidas))
    dfLimpio = dfLimpio.withColumn("id", dfLimpio.id.cast(IntegerType()))
    dfLimpio = dfLimpio.filter(dfLimpio.id.isNotNull())
    dfLimpio = dfLimpio.filter(length(dfLimpio.id.cast("string")) == 8)
    dfLimpio = dfLimpio.dropDuplicates(["id"])
    return dfLimpio

dfLimpio = limpiarCategoriaId(df)

# %% [markdown]
# ## <center><font color='#1E90FF'>Eliminación de valores inválidos <a id="valores-invalidos"></a></font></center>
# 
# El siguiente paso consistió en identificar y eliminar valores inválidos en columnas de texto esenciales (**titulo**, **director**, **reparto**, **descripcion** y **pais**), ya que se observó que contenían información incorrecta como URLs, correos electrónicos, números aislados o fechas. Para lograr esto, definí patrones mediante expresiones regulares (regex) que detectan específicamente:
# 
# - **URLs**: cadenas que comienzan con `http://` o `https://`.
# - **Correos electrónicos**: formatos habituales que contienen un símbolo `@` seguido de un dominio.
# - **Números puros**: cadenas compuestas exclusivamente por dígitos, sin caracteres alfabéticos.
# - **Fechas textuales**: fechas escritas en inglés como `"January 31, 2019"`.
# 
# La solución consistió en construir filtros booleanos basados en estas expresiones regulares para detectar filas inválidas, y posteriormente eliminar dichas filas del DataFrame. Repetí este proceso para cada columna relevante, asegurando así que todas las filas conservadas contuvieran información coherente y válida para el análisis.

# %%
columnas = ["director", "reparto", "descripcion", "pais"]
patrones = [
    r"(?i)^https?://",
    r"(?i)^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$",
    r"^[0-9]+$",
    r"^(January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{1,2},\\s+\\d{4}$"
]
condiciones = None
for columna in columnas:
    for patron in patrones:
        nueva_condicion = col(columna).rlike(patron)
        if condiciones is None:
            condiciones = nueva_condicion
        else:
            condiciones = condiciones | nueva_condicion

dfLimpio = dfLimpio.filter(~condiciones)


# %% [markdown]
# El campo titulo se analiza de manera diferente porque, a diferencia de los otros, puede contener números de forma válida. En algunas producciones, los títulos incluyen caracteres numéricos como parte del nombre oficial, por ejemplo, "300",

# %%
columnas = ["titulo"]
patrones = [
    r"(?i)^https?://",  # URL
    r"(?i)^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$",  # correo
    r"^(January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{1,2},\\s+\\d{4}$" # Fechas en inglés
]
condiciones = None
for columna in columnas:
    for patron in patrones:
        nueva_condicion = col(columna).rlike(patron)
        if condiciones is None:
            condiciones = nueva_condicion
        else:
            condiciones = condiciones | nueva_condicion

dfLimpio = dfLimpio.filter(~condiciones)

# %% [markdown]
# ## <center><font color='#1E90FF'>Estandarización de duración <a id="duracion"></a></font></center>
# 
# En este punto, procedí a estandarizar la columna `duracion`, separando claramente las duraciones de películas (en minutos) y el número de temporadas de las series en columnas independientes, facilitando así futuras operaciones numéricas:
# 
# - **`duracionMin`**: Almacena duración en minutos (tipo entero) para películas.
# - **`numTemporadas`**: Almacena número de temporadas (tipo entero) para series.
# 
# Para lograr esto, definí expresiones regulares que extraen exclusivamente números seguidos por «min» o «Season(s)», asignando estos valores en las nuevas columnas mediante las funciones `when()`, `rlike()` y `regexp_extract()`. Finalmente, eliminé filas con valores inesperados en la columna original.
# 

# %%
regexMin = r"(\d+)\s+min$"
regexSeasons = r"(\d+)\s+Seasons?$"

dfLimpio = dfLimpio.withColumn(
    "duracionMin",
    when(dfLimpio.duracion.rlike(regexMin), regexp_extract(dfLimpio.duracion, regexMin, 1).cast("int"))
).withColumn(
    "numTemporadas",
    when(dfLimpio.duracion.rlike(regexSeasons), regexp_extract(dfLimpio.duracion, regexSeasons, 1).cast("int"))
)

# Ahora filtrar
dfLimpio = dfLimpio.filter((dfLimpio.duracionMin.isNotNull()) | (dfLimpio.numTemporadas.isNotNull()))

# %% [markdown]
# ## <center><font color='#1E90FF'>Validación de fechas en Netflix <a id="fechas-netflix"></a></font></center>
# 
# En esta fase, me centré en limpiar y validar la columna `estrenoNetflix`, la cual representa las fechas en que los títulos fueron estrenados en Netflix. Debido a que los datos originales podían contener formatos variados o inconsistentes, realicé lo siguiente:
# 
# - **Estandarización de formato:** Utilicé expresiones como `"January 1, 2020"` para convertir textos a fechas reconocibles por Spark, eliminando espacios innecesarios y errores comunes.
# - **Conversión a tipo fecha:** Transformé los textos válidos al formato de fecha reconocido formalmente por Spark mediante `to_date()` con el formato `"MMMM d, yyyy"`.
# - **Filtrado:** Conservé únicamente aquellas filas cuya conversión resultó exitosa, descartando registros con fechas inválidas o inconsistentes.
# 
# Con esto garantizo que todas las fechas almacenadas en `estrenoNetflix` sean válidas y uniformes, facilitando significativamente las operaciones analíticas posteriores.
# 
# 
# ---
# 
# ### Características del código
# 
# - **Uso de `trim()` y `to_date()`:** Garantizan uniformidad y reconocimiento efectivo de las fechas.
# - **Filtro basado en conversión:** Permite mantener únicamente fechas válidas y consistentes para análisis temporales.
# 

# %%
# Primera vez que se convierte un campo de texto en fecha válida
dfLimpio = dfLimpio.withColumn("estrenoNetflix", trim(dfLimpio.estrenoNetflix))
dfLimpio = dfLimpio.filter(to_date(dfLimpio.estrenoNetflix, "MMMM d, yyyy").isNotNull())

# %% [markdown]
# ## <center><font color='#1E90FF'>Validación del año de estreno <a id="ano-estreno"></a></font></center>
# 
# El siguiente paso fue validar la columna **anioEstrenoReal**, que registra el año original de estreno (cine o TV). Detecté valores incorrectos como texto, años imposibles o nulos. Para garantizar coherencia y calidad, realicé lo siguiente:
# 
# - **Conversión a numérico**: Transformé la columna a tipo entero, marcando automáticamente como nulos los valores no numéricos o inconsistentes.
# - **Filtrado de nulos**: Eliminé filas cuyo año resultó nulo tras la conversión (años inválidos o texto).
# 
# Este proceso eliminó errores típicos como textos en campos numéricos o años extremos improbables, dejando un conjunto de datos con años válidos, numéricos y consistentes.

# %%
dfLimpio = dfLimpio.withColumn("anioEstrenoReal", dfLimpio.anioEstrenoReal.cast("int"))
dfLimpio = dfLimpio.filter(dfLimpio.anioEstrenoReal.isNotNull())

# %% [markdown]
# ## <center><font color='#1E90FF'>Normalización de subcategoría <a id="subcategoria"></a></font></center>
# 
# El siguiente paso fue limpiar y normalizar la columna `subcategoria`, ya que contenía datos mixtos: clasificaciones por edad (por ejemplo, `"PG-13"`), o valores redundantes de duración en minutos o temporadas. Para solucionar esto:
# 
# - Definí una lista con clasificaciones de audiencia válidas (`"PG"`, `"TV-MA"`, `"R"`, etc.).
# - Apliqué la función `trim()` para eliminar espacios extra.
# - Usé sentencias `when()` encadenadas para clasificar los valores:
#   - Si el valor era una clasificación válida, lo mantuve tal cual.
#   - Si coincidía con el patrón de minutos (`regexMin`), extraje el valor numérico.
#   - Si coincidía con temporadas (`regexSeasons`), extraje igualmente el número.
#   - Si no cumplía ninguno de estos casos, asigné valor nulo.
# - Finalmente, filtré para conservar únicamente filas con valores válidos tras esta normalización.
# 
# **Con esta estrategia, ahora la columna `subcategoria` contiene solo clasificaciones claras o valores numéricos, eliminando casos confusos o irrelevantes.**
# 
# ---
# 

# %%
# Primera vez que se limpia la subcategoría
categorias_validas = ["G", "PG", "PG-13", "R", "NC-17", "NR", "UR","TV-G", "TV-PG", "TV-Y", "TV-Y7", "TV-Y7-FV", "TV-14", "TV-MA"]
dfLimpio = dfLimpio.withColumn("subcategoria", trim(col("subcategoria")))

dfLimpio = dfLimpio.withColumn(
    "subcategoria",
    when(col("subcategoria").isin(categorias_validas), col("subcategoria"))
    .when(col("subcategoria").rlike(regexMin), regexp_extract(col("subcategoria"), regexMin, 1).cast("int"))
    .when(col("subcategoria").rlike(regexSeasons), regexp_extract(col("subcategoria"), regexSeasons, 1).cast("int"))
    .otherwise(None)
)

dfLimpio = dfLimpio.filter(col("subcategoria").isNotNull())

# %% [markdown]
# ## <center><font color='#1E90FF'>Limpieza de género <a id="genero"></a></font></center>
# 
# La siguiente etapa consistió en limpiar la columna `género`, que incluye listas de géneros separadas por comas (ej.: `"Comedy, Drama"`). Para mantener la calidad del análisis y asegurar la consistencia, realicé los siguientes pasos:
# 
# - **Eliminación de valores vacíos o nulos:** Removí los registros sin información en la columna género, asumiendo que cada título debe contar con al menos un género válido.
# 
# - **Normalización del formato:** Aunque idealmente validaría los géneros frente a una lista predefinida (ej.: `"Comedy"`, `"Drama"`, `"Action"`), aquí asumí que los géneros restantes eran válidos y me enfoqué en asegurar su formato correcto.
# 
# Tras este proceso, la columna `género` quedó limpia, completa y preparada para transformaciones adicionales en fases posteriores.
# 
# ---
# 

# %%
# Primera vez que se divide una columna en un array y se filtran valores inválidos
dfLimpio = dfLimpio.filter(dfLimpio.genero.isNotNull())

# %% [markdown]
# ### Resumen de transformaciones
# 
# Finalmente, se muestra un ejemplo de cómo comparar el DataFrame original versus el DataFrame limpio, revisando la cantidad de filas y el schema para verificar el resultado global de la limpieza.
# 

# %%
resultado = contar_nulos(dfLimpio)
print(resultado)

print("----- ANTES DE LA LIMPIEZA -----")
print(f"Filas totales: {df.count()}")
print("\n----- DESPUÉS DE LA LIMPIEZA -----")
print(f"Filas totales: {dfLimpio.count()}")
dfLimpio.show(5)


# %% [markdown]
# ## <center><font color='#1E90FF'>Conversión de tipos de datos <a id="conversion"></a></font></center>
# 
# El siguiente paso fue realizar una última conversión de tipos en el DataFrame (`dfLimpio`), garantizando que todas las columnas tuviesen el formato correcto antes de iniciar el análisis exploratorio. Para ello:
# 
# - **`duracion`**: Convertí esta columna a tipo entero, extrayendo únicamente el valor numérico inicial (minutos o temporadas), aunque ya disponíamos de columnas específicas (`duracionMin`, `numTemporadas`).
# 
# - **`anioEstrenoReal`**: Confirmé que se mantuviera como tipo entero tras la limpieza previa.
# 
# - **`estrenoNetflix`**: Transformé esta columna a formato fecha (`DateType`) utilizando el patrón `"MMMM d, yyyy"`, esencial para futuros análisis temporales.
# 
# Finalmente, comprobé que todas las conversiones se aplicaron correctamente revisando el esquema actualizado del DataFrame.
# 

# %%
dfLimpio = dfLimpio.withColumn("duracion", split(dfLimpio.duracion, " ")[0].cast("int")) \
       .withColumn("anioEstrenoReal", dfLimpio.anioEstrenoReal.cast("int")) \
       .withColumn("estrenoNetflix", to_date(dfLimpio.estrenoNetflix, "MMMM d, yyyy"))

for columna in dfLimpio.schema.fields:
    print(f"{columna.name}: {columna.dataType}")

# %% [markdown]
# 
# 
# ---
# 
# 
# ## **FASE DE EXPLORACIÓN DE DATOS**
# 
# ---
# 
# 

# %% [markdown]
# ## <center><font color='#1E90FF'>Duración promedio por país <a id="duracion-pais"></a></font></center>
# 
# Una consulta clave del análisis fue determinar la duración promedio de las películas según el país productor. Sin embargo, el campo `pais` incluye casos de coproducciones (varios países separados por coma), lo que nos lleva a dos enfoques posibles:
# 
# **1. Promedio por país combinado:**
# - Cada combinación de países cuenta como un único grupo (ej.: `"Estados Unidos, India"` sería un grupo independiente).
# 
# **2. Promedio por país individual:**
# - Cada país involucrado se considera por separado, distribuyendo la duración a cada país participante.
# 
# Para abordar ambos casos realicé lo siguiente:
# 
# ### **Cálculo por país combinado**
# - Utilicé directamente la columna `pais` original, promediando la duración en minutos (`duracionMin`) solo para películas (descartando series, que no tienen minutos asociados).
# 
# ### **Cálculo por país individual**
# - Dividí la columna de países mediante `split()` generando múltiples filas con `explode()`, asignando así una fila por país individual.
# - Sobre este DataFrame ampliado, recalculé la duración media en minutos.
# - Presenté estos resultados en minutos y segundos, mejorando su claridad visual.
# 
# Finalmente, comparé ambos enfoques destacando las diferencias entre medir por país combinado o individual, demostrando cómo influyen las coproducciones en métricas clave como la duración promedio.
# 
# ---

# %%
mediaDuracion = dfLimpio.groupBy("pais").agg(round(avg("duracionMin"),2).alias("mediaDuracion"))
mediaDuracion.show(10, truncate = False)

# %%
mediaDuracion = mediaDuracion.withColumn("minutos", floor(col("mediaDuracion"))) \
                             .withColumn("segundos", round((col("mediaDuracion") - col("minutos")) * 60, 0).cast("int")) \
                             .withColumn("duracionFormateada", expr("concat(minutos, ' min ', segundos, ' sec')")) \
                             .drop("mediaDuracion", "minutos", "segundos")

mediaDuracion.show(10, truncate=False)


# %%
df_paisIndividual = dfLimpio.withColumn("paisIndividual", explode(split(dfLimpio.pais, ", ")))
df_paisIndividual.select("paisIndividual").show(10)

# %%
mediaDuracionSiParticipa = df_paisIndividual.groupBy("paisIndividual").agg(round(avg("duracion"),2).alias("mediaDuracionSiParticipa"))
mediaDuracionSiParticipa.orderBy("paisIndividual").show(3)

# %%
mediaDuracionSiParticipa = mediaDuracionSiParticipa.withColumn("minutos", floor(col("mediaDuracionSiParticipa"))) \
                             .withColumn("segundos", round((col("mediaDuracionSiParticipa") - col("minutos")) * 60, 0).cast("int")) \
                             .withColumn("duracionFormateada", expr("concat(minutos, ' min ', segundos, ' sec')")) \
                             .drop("mediaDuracionSiParticipa", "minutos", "segundos")

mediaDuracionSiParticipa.show(10, truncate=False)


# %%
# Aquí se comprueba que había diferencia
paises = ["United States", "France", "India", "Spain"]

# Filtrar los DataFrames para incluir solo los países seleccionados
mediaDuracionFiltrada = mediaDuracion.filter(mediaDuracion.pais.isin(paises))
mediaDuracionSiParticipaFiltrada = mediaDuracionSiParticipa.filter(mediaDuracionSiParticipa.paisIndividual.isin(paises))

# Hacer un join entre ambos DataFrames en la columna de país
comparacionDuracion = mediaDuracionFiltrada.alias("general").join(
    mediaDuracionSiParticipaFiltrada.alias("participa"),
    mediaDuracionFiltrada.pais == mediaDuracionSiParticipaFiltrada.paisIndividual,
    "inner"
).select(
    mediaDuracionFiltrada.pais.alias("Pais"),
    mediaDuracionFiltrada.duracionFormateada.alias("Media General"),
    mediaDuracionSiParticipaFiltrada.duracionFormateada.alias("Media Si Participa")
)

comparacionDuracion.show(truncate=False)

# %% [markdown]
# ## <center><font color='#1E90FF'>Actor más frecuente en musicales <a id="actor-musicales"></a></font></center>
# 
# Otra consulta clave consistió en identificar al actor más frecuente en películas relacionadas con la temática *"music"* cuya duración supera los 90 minutos. El procedimiento que realicé para resolver este análisis fue:
# 
# 1. **Filtrado del DataFrame**: Seleccioné únicamente películas (*categoria = "Movie"*) con la palabra *"music"* en su descripción (previamente convertida a minúsculas) y duración superior a 90 minutos.
# 2. **Separación del reparto**: Dividí el campo reparto usando `split()` y `explode()`, generando filas individuales por cada actor.
# 3. **Conteo de apariciones**: Agrupé los resultados por actor, contando cuántas veces aparecía cada uno dentro del conjunto filtrado.
# 4. **Ordenación y selección**: Ordené el resultado por frecuencia descendente, identificando al actor con más apariciones.
# 
# El actor más frecuente bajo estas condiciones fue **Note Chern-Yim**, quien participó en *2 películas* del subconjunto analizado, aunque cabe señalar que existieron otros actores con la misma cantidad de apariciones.

# %%
dfLimpio = dfLimpio.filter((dfLimpio.categoria=="Movie") & (lower(dfLimpio.descripcion).contains("music")) & (dfLimpio.duracion > 90))

masApariciones = dfLimpio.withColumn("actorIndividual", explode(split(dfLimpio.reparto, ", ")))
masApariciones = masApariciones.groupBy("actorIndividual").agg(count("actorIndividual").alias("numeroActuaciones"))
masApariciones.orderBy("numeroActuaciones", ascending=False).show(30)

actorMasApariciones = masApariciones.orderBy("numeroActuaciones", ascending=False).collect()[0][0]

print(f"Hay muchos actores con 2 actuaciones, pero en este caso nos vamos a quedar con el primero que sale que es {actorMasApariciones}")

# %% [markdown]
# ## <center><font color='#1E90FF'>Diferencia semanal entre películas <a id="diferencia"></a></font></center>
# 
# En esta fase, analicé la diferencia semanal entre películas con temática "music" de duración mayor a 90 minutos, protagonizadas por el actor más frecuente. El objetivo fue determinar el intervalo temporal entre su primera y última aparición en esta categoría específica. Para ello:
# 
# - Tomé como referencia el DataFrame filtrado (`df_music`) obtenido anteriormente, que ya cumplía los requisitos definidos.
# - Filtré específicamente los títulos protagonizados por el actor más frecuente identificado.
# - Dado que solo disponía de fechas de estreno en Netflix y años originales, decidí obtener fechas de estreno reales desde IMDb usando web scraping (`requests` y `BeautifulSoup`).
# - Obtuve las fechas exactas para cada película consultando IMDb (por ejemplo: *"Holy Man 2"*: 24 de diciembre de 2008, *"Holy Man 3"*: 5 de abril de 2010, ilustrativamente).
# - Asocié estas fechas al DataFrame filtrado mediante un `join`.
# - Convertí finalmente estas fechas al tipo `DateType` de Spark para realizar cálculos de intervalo con precisión.
# 
# Con estos pasos, pude determinar con exactitud la diferencia en semanas entre las fechas extremas de aparición del actor en películas relacionadas con música.
# 
# ### ¿Cómo lo resolví?
# Obtuve las fechas reales de estreno mediante *scraping* con `BeautifulSoup` desde IMDb y las asocié al DataFrame, calculando luego la diferencia temporal entre estas fechas reales para cuantificar la dispersión en semanas.
# 
# ### Características del código
# - **Web scraping con `requests` y `BeautifulSoup`**: Permite incorporar datos externos confiables (IMDb).
# - **Uso de `join` y `to_date()`**: Facilitan un cálculo preciso y claro del intervalo temporal entre producciones.
# 

# %%
holy = dfLimpio.filter(dfLimpio.reparto.contains(actorMasApariciones))

# %%
# Diccionario con las películas y sus URLs de IMDb
urls = {
    'Holy Man 2': 'https://www.imdb.com/title/tt3051874/releaseinfo/',
    'Holy Man 3': 'https://www.imdb.com/title/tt1720154/releaseinfo/'
}

# Función para extraer la fecha de estreno desde IMDb
def obtener_fecha_estreno(url):
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        # (Basado en la estructura actual de IMDb)
        fecha_span = soup.find_all("span", class_="ipc-metadata-list-item__list-content-item")

        if fecha_span:
            release_date = fecha_span[0].text.strip()
            return release_date
        else:
            return None
    else:
        print(f"Error al acceder a la página {url}. Código: {response.status_code}")
        return None

# Crear una lista de tuplas (título, fecha_estreno)
data = []
for titulo, url in urls.items():
    fecha_estreno = obtener_fecha_estreno(url)
    if fecha_estreno:
        data.append((titulo, fecha_estreno))


dfFechasReales = spark.createDataFrame(data, ["titulo", "fechaEstrenoReal"])

# Mostrar el DataFrame
dfFechasReales.show()

# %%
dfJoin = holy.join(dfFechasReales, on='titulo', how='left')
dfJoin.show()

dfJoin = dfJoin.withColumn(
    "fechaEstrenoReal",
    to_date(dfJoin.fechaEstrenoReal, "MMMM d, yyyy")
)

dfDiff = dfJoin.select(
    datediff(
        max("fechaEstrenoReal"),
        min("fechaEstrenoReal")
    ).alias("diferenciaDias")
)

dfDiff.show()

print(f"Entre la primera película de {actorMasApariciones} según el filtro aplicado, es decir entre {dfFechasReales.collect()[0][0]} y {dfFechasReales.collect()[1][0]} han pasado {dfDiff.collect()[0][0]/7} semanas")

# %% [markdown]
# ## <center><font color='#1E90FF'>Transformación de género a array <a id="genero-array"></a></font></center>
# 
# El siguiente paso fue transformar la columna `genero`, originalmente en texto plano, en una estructura tipo lista (array) que facilitase futuras consultas y filtrados específicos por género. Para ello, usé la función `split()` de Spark, especificando la coma como separador, obteniendo así una lista de géneros individuales por cada registro.
# 
# Este proceso convirtió valores como `"Comedy, Drama"` en un array del tipo `["Comedy", "Drama"]`, facilitando análisis posteriores que requieran acceder individualmente a cada género (por ejemplo, contar apariciones o filtrar por género específico).
# 
# ### ¿Cómo lo resolví?
# Convertí el texto plano en una estructura de array claramente manejable usando la función `split()`. Esto permitió una mejor organización y aprovechamiento de la información por género en análisis posteriores.

# %%
df = df.withColumn("generosArray", split(df.genero, ","))

df.select("genero", "generosArray").show(5, truncate=False)

# %% [markdown]
# ## <center><font color='#1E90FF'>Conteo por número de países <a id="conteo-paises"></a></font></center>
# 
# Otra consulta relevante fue determinar cuántas producciones del catálogo corresponden a un único país, frente a cuántas fueron realizadas en colaboración entre dos o más países. Para ello, realicé lo siguiente:
# 
# ### Creación de una columna auxiliar
# Implementé una función UDF sencilla (`contarPaises`) que cuenta el número de países listados en la columna `pais`, basándose en el número de comas presentes más uno.
# 
# ### Aplicación de la función
# Añadí la columna `numeroPaises` al DataFrame, identificando fácilmente cuántos países participaron en cada producción.
# 
# ### Conteo de producciones
# Separé las **producciones individuales** (únicamente un país involucrado) de las **coproducciones** (dos o más países).  
# 
# **Ejemplo:**  
# - Un registro con `pais = "India"` tendría `numeroPaises = 1`.  
# - Un registro con `pais = "United States, Mexico"` tendría `numeroPaises = 2`.  
# ---
# 

# %%
def contarPaises(paises):
  return paises.count(",") + 1

contarPaises = udf(contarPaises, IntegerType())

dfLimpio = dfLimpio.withColumn("numeroPaises", contarPaises(dfLimpio.pais))
producciones1pais = dfLimpio.filter(dfLimpio.numeroPaises > 1).count()
produccionesVariosPaises = dfLimpio.filter(dfLimpio.numeroPaises == 1).count()


print(f"En {producciones1pais} películas solamente había un país involucrado en su producción y en {produccionesVariosPaises} películas había más de un país involucrado")

# %% [markdown]
# ## <center><font color='#1E90FF'>Exportación a Parquet <a id="parquet"></a></font></center>
# 
# Finalmente, tras completar las etapas de limpieza y análisis preliminar, exporté el DataFrame resultante a un archivo en formato **Parquet**. Este formato ofrece ventajas significativas para entornos analíticos intensivos
# 
# Realicé esta exportación indicando la opción `mode="overwrite"` para asegurar que, si existía un archivo previo en la ruta especificada, fuera sustituido directamente por la nueva versión.

# %%
parquet = "/tmp/netflixLimpioFiltrado.parquet"
dfLimpio.write.parquet(parquet, mode="overwrite")
print(f"DataFrame limpio guardado como Parquet en {parquet}")



