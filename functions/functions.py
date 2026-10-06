#Importar bibliotecas
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType, DateType
)

#Definir los esquemas
CICLISTA_SCHEMA = StructType([
    StructField("cedula", LongType(), True),
    StructField("nombre", StringType(), True),
    StructField("provincia", StringType(), True)
])

RUTA_SCHEMA = StructType([
    StructField("codigo_ruta", LongType(), True),
    StructField("nombre_ruta", StringType(), True),
    StructField("kilometros", DoubleType(), True)
])

ACTIVIDAD_SCHEMA = StructType([
    StructField("codigo_ruta", LongType(), True),
    StructField("cedula", LongType(), True),
    StructField("fecha", DateType(), True)
])

# Carga de los datasets

def cargar_ciclistas(spark, ruta_archivo):
    return spark.read.csv(
        ruta_archivo,
        header=False,
        schema=CICLISTA_SCHEMA
    )


def cargar_rutas(spark, ruta_archivo):
    return spark.read.csv(
        ruta_archivo,
        header=False,
        schema=RUTA_SCHEMA
    )


def cargar_actividades(spark, ruta_archivo):
    return spark.read.csv(
        ruta_archivo,
        header=False,
        schema=ACTIVIDAD_SCHEMA,
        dateFormat="yyyy-MM-dd"
    )

# Unir todos los datos

def unir_datos(df_ciclistas, df_rutas, df_actividades):
    actividades_rutas = df_actividades.join(
        df_rutas,
        on="codigo_ruta",
        how="left"
    )
    df_datos = df_ciclistas.join(
        actividades_rutas,
        on="cedula",
        how="left"
    )
    return df_datos

# Agregaciones parciales

def total_km_por_persona(df_datos):
    return (
        df_datos
        .groupBy("cedula", "nombre", "provincia")
        .agg(
            F.coalesce(F.sum("kilometros"), F.lit(0.0)).alias("total_km"),
            F.countDistinct("fecha").alias("dias_activos")
        )
        .withColumn(
            "promedio_diario_km",
            F.when(
                F.col("dias_activos") > 0,
                F.col("total_km") / F.col("dias_activos")
            ).otherwise(F.lit(0.0))
        )
    )


def total_km_por_provincia(df_datos):
    return (
        df_datos
        .groupBy("provincia")
        .agg(
            F.coalesce(F.sum("kilometros"), F.lit(0.0)).alias("total_km")
        )
    )


def total_km_por_dia(df_datos):
    return (
        df_datos
        .filter(F.col("fecha").isNotNull())
        .groupBy("fecha")
        .agg(
            F.coalesce(F.sum("kilometros"), F.lit(0.0)).alias("total_km")
        )
    )

# Resultados finales

def top_por_total_km(df_personas, n=5):
    ventana = Window.partitionBy("provincia").orderBy(
        F.col("total_km").desc(),
        F.col("cedula").asc()
    )

    return (
        df_personas
        .filter(F.col("dias_activos") > 0)
        .withColumn(
            "posicion",
            F.row_number().over(ventana)
        )
        .filter(F.col("posicion") <= n)
        .orderBy("provincia", "posicion")
    )

def top_por_promedio_km(df_personas, n=5):
    ventana = Window.partitionBy("provincia").orderBy(
        F.col("promedio_diario_km").desc(),
        F.col("cedula").asc()
    )

    return (
        df_personas
        .filter(F.col("dias_activos") > 0)
        .withColumn(
            "posicion",
            F.row_number().over(ventana)
        )
        .filter(F.col("posicion") <= n)
        .orderBy("provincia", "posicion")
    )
