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

# %%
def contar_nulos(df):
    nulos_df = df.select([(sum(col(c).isNull().cast("int")).alias(c)) for c in df.columns])

    resultado = nulos_df.toPandas().transpose().reset_index().rename(columns={'index': 'columna', 0: 'nulos'})
    return resultado

resultado = contar_nulos(df)
print(resultado)

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


regexMin = r"(\d+)\s+min$"
regexSeasons = r"(\d+)\s+Seasons?$"

dfLimpio = dfLimpio.withColumn(
    "duracionMin",
    when(dfLimpio.duracion.rlike(regexMin), regexp_extract(dfLimpio.duracion, regexMin, 1).cast("int"))
).withColumn(
    "numTemporadas",
    when(dfLimpio.duracion.rlike(regexSeasons), regexp_extract(dfLimpio.duracion, regexSeasons, 1).cast("int"))
)

dfLimpio = dfLimpio.filter((dfLimpio.duracionMin.isNotNull()) | (dfLimpio.numTemporadas.isNotNull()))

dfLimpio = dfLimpio.withColumn("estrenoNetflix", trim(dfLimpio.estrenoNetflix))
dfLimpio = dfLimpio.filter(to_date(dfLimpio.estrenoNetflix, "MMMM d, yyyy").isNotNull())


dfLimpio = dfLimpio.withColumn("anioEstrenoReal", dfLimpio.anioEstrenoReal.cast("int"))
dfLimpio = dfLimpio.filter(dfLimpio.anioEstrenoReal.isNotNull())


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

dfLimpio = dfLimpio.filter(dfLimpio.genero.isNotNull())
dfLimpio)
print(resultado)

print("----- ANTES DE LA LIMPIEZA -----")
print(f"Filas totales: {df.count()}")
print("\n----- DESPUÉS DE LA LIMPIEZA -----")
print(f"Filas totales: {dfLimpio.count()}")
dfLimpio.show(5)


dfLimpio = dfLimpio.withColumn("duracion", split(dfLimpio.duracion, " ")[0].cast("int")) \
       .withColumn("anioEstrenoReal", dfLimpio.anioEstrenoReal.cast("int")) \
       .withColumn("estrenoNetflix", to_date(dfLimpio.estrenoNetflix, "MMMM d, yyyy"))

for columna in dfLimpio.schema.fields:
    print(f"{columna.name}: {columna.dataType}")


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


# %%
dfLimpio = dfLimpio.filter((dfLimpio.categoria=="Movie") & (lower(dfLimpio.descripcion).contains("music")) & (dfLimpio.duracion > 90))

masApariciones = dfLimpio.withColumn("actorIndividual", explode(split(dfLimpio.reparto, ", ")))
masApariciones = masApariciones.groupBy("actorIndividual").agg(count("actorIndividual").alias("numeroActuaciones"))
masApariciones.orderBy("numeroActuaciones", ascending=False).show(30)

actorMasApariciones = masApariciones.orderBy("numeroActuaciones", ascending=False).collect()[0][0]

print(f"Hay muchos actores con 2 actuaciones, pero en este caso nos vamos a quedar con el primero que sale que es {actorMasApariciones}")



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


# %%
df = df.withColumn("generosArray", split(df.genero, ","))

df.select("genero", "generosArray").show(5, truncate=False)


# %%
def contarPaises(paises):
  return paises.count(",") + 1

contarPaises = udf(contarPaises, IntegerType())

dfLimpio = dfLimpio.withColumn("numeroPaises", contarPaises(dfLimpio.pais))
producciones1pais = dfLimpio.filter(dfLimpio.numeroPaises > 1).count()
produccionesVariosPaises = dfLimpio.filter(dfLimpio.numeroPaises == 1).count()


print(f"En {producciones1pais} películas solamente había un país involucrado en su producción y en {produccionesVariosPaises} películas había más de un país involucrado")


# %%
parquet = "/tmp/netflixLimpioFiltrado.parquet"
dfLimpio.write.parquet(parquet, mode="overwrite")
print(f"DataFrame limpio guardado como Parquet en {parquet}")



