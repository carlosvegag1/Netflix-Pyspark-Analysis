# 📺 Análisis de Contenido de Netflix con PySpark

<p align="center">
<a href="https://github.com/carlosvegag1/Netflix-Data-Cleaning"><img src="https://i.imgur.com/rekbD3q.png" width="80%"></a>
</p>

## 🎬 Explorando el Catálogo de Netflix con Big Data

Este proyecto se centra en la limpieza y análisis del catálogo de Netflix utilizando **PySpark**, una poderosa herramienta de procesamiento distribuido. A lo largo del estudio, se implementan diversas técnicas de limpieza de datos, normalización y transformación para obtener información estructurada y precisa sobre el contenido disponible en la plataforma.

## 📌 Contenido del Proyecto

Este repositorio contiene un análisis detallado con PySpark, incluyendo:

✅ **Limpieza avanzada de datos con regex y filtrado de valores anómalos.**  
✅ **Normalización de duraciones y categorías (Movies vs. TV Shows).**  
✅ **Optimización de consultas con SparkSQL y uso eficiente de DataFrames.**  
✅ **Uso de Web Scraping para obtener fechas de estreno reales desde IMDb.**  
✅ **Exportación del dataset final en formato Parquet para optimización.**  

Se han utilizado fragmentos de código clave para demostrar la eficiencia y robustez del procesamiento con PySpark.

---
## Fragmentos Destacados del Código

### 📅 Web Scraping para Obtener Fechas de Estreno Reales desde IMDb
```python
import requests
from bs4 import BeautifulSoup

def obtener_fecha_estreno(url):
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        fecha_span = soup.find_all("span", class_="ipc-metadata-list-item__list-content-item")
        if fecha_span:
            return fecha_span[0].text.strip()
    return None
```
🔹Este fragmento permite extraer automáticamente fechas de estreno reales de IMDb, enriqueciendo el dataset más allá de los datos originales.

### 🎭 Diferenciación entre Películas y Series con Expresiones Regulares
```python
from pyspark.sql.functions import regexp_extract, when

regexMin = r"(\d+)\s+min$"
regexSeasons = r"(\d+)\s+Seasons?$"

dfLimpio = dfLimpio.withColumn(
    "duracionMin",
    when(dfLimpio.duracion.rlike(regexMin), regexp_extract(dfLimpio.duracion, regexMin, 1).cast("int"))
).withColumn(
    "numTemporadas",
    when(dfLimpio.duracion.rlike(regexSeasons), regexp_extract(dfLimpio.duracion, regexSeasons, 1).cast("int"))
)
```
🔹 Esto permite separar de forma inteligente la duración en minutos (para películas) y el número de temporadas (para series), mejorando la usabilidad de los datos.

### ⏳ Comparación de Duración Media entre Producciones Individuales y Coproducciones
```python
from pyspark.sql.functions import split, explode, avg, round

# Explodemos la columna 'pais' para obtener cada país individualmente
df_paisIndividual = dfLimpio.withColumn("paisIndividual", explode(split(dfLimpio.pais, ", ")))

# Cálculo de la duración media por país y por coproducción
mediaDuracionPorPais = dfLimpio.groupBy("pais").agg(round(avg("duracionMin"), 2).alias("mediaDuracion"))
mediaDuracionSiParticipa = df_paisIndividual.groupBy("paisIndividual").agg(round(avg("duracionMin"), 2).alias("mediaDuracionSiParticipa"))

# Comparación de duración entre producciones individuales y coproducciones
comparacionDuracion = mediaDuracionPorPais.alias("general").join(
    mediaDuracionSiParticipa.alias("participa"),
    mediaDuracionPorPais.pais == mediaDuracionSiParticipa.paisIndividual,
    "inner"
).select(
    col("general.pais").alias("País"),
    col("general.mediaDuracion").alias("Duración Media (Producción Nacional)"),
    col("participa.mediaDuracionSiParticipa").alias("Duración Media (Si Participa)")
)

comparacionDuracion.show(truncate=False)
```
---

##  Instalación y Uso

### 1️ Clonar el Repositorio
```bash
git clone https://github.com/carlosvegag1/netflix-data-cleaning.git
cd netflix-data-cleaning
```
### 2️ Crear un Entorno Virtual e Instalar Dependencias
```bash
python -m venv env
source env/bin/activate  # (Windows: env\Scripts\activate)
pip install -r requirements.txt
```
### 3️ Ejecutar el Notebook
```bash
Abre Jupyter Notebook y ejecuta netflix_data_cleaning.ipynb
jupyter notebook
```

##  Otras formas de visualizar el análisis  
Si prefieres explorar el análisis sin necesidad de ejecutar código, puedes acceder aquí:  

📄 **Versión en HTML**  
🔗 **[Netflix-Data-Cleaning.html](https://carlosvegag1.github.io/netflix-data-cleaning/netflix_analysis.html)**  

📄 **Versión en PDF**  
🔗 **[Netflix-Data-Cleaning.pdf](https://github.com/carlosvegag1/netflix-data-cleaning/blob/main/docs/netflix_analysis.pdf)**

---

## 📥 Origen de los Datos
Los datos analizados en este proyecto provienen del repositorio de **DataCamp** sobre limpieza de datos en PySpark. Se han descargado, procesado y enriquecido con información adicional obtenida mediante *web scraping* de IMDb.

🔗 **[Dataset Original en DataCamp](https://github.com/datacamp/data-cleaning-with-pyspark-live-training)**

Para más información sobre el dataset y otros proyectos de limpieza de datos, visita la fuente oficial de DataCamp.

---

### 🤝 Compalte
Este proyecto ha sido desarrollado como parte de mi formación en **Ciencia de Datos** con PySpark. Si tienes sugerencias o quieres contribuir, ¡serás bienvenido!

**⭐ Si te resulta útil, no olvides darle una estrella al repositorio.**

### 📬 Contacto
📌 **LinkedIn:** [Carlos Vega González](https://www.linkedin.com/in/carlos-vega-gonzalez/)  
📧 **Email:** carlosvegagonzalez1@gmail.com  
