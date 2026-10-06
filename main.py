import os
import sys
from pyspark.sql import SparkSession
from functions.functions import (
    cargar_ciclistas, cargar_rutas, cargar_actividades,
    unir_datos, total_km_por_persona,
    top_por_total_km, top_por_promedio_km
)

TOP_N = 5


def validar_argumentos(args):
    if len(args) != 4:
        sys.exit("Uso: spark-submit main.py ciclista.csv ruta.csv actividad.csv")
    for ruta in args[1:]:
        if not os.path.isfile(ruta):
            sys.exit(f"Error: no existe el archivo '{ruta}'")


def mostrar_ranking(titulo, ranking):
    print(f"\n=== {titulo} ===")
    ranking.orderBy("provincia", "posicion").show(50, truncate=False)


def main():
    validar_argumentos(sys.argv)

    ruta_ciclistas = sys.argv[1]
    ruta_rutas = sys.argv[2]
    ruta_actividades = sys.argv[3]

    spark = SparkSession.builder.appName("Tarea1").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df_ciclistas = cargar_ciclistas(spark, ruta_ciclistas)
    df_rutas = cargar_rutas(spark, ruta_rutas)
    df_actividades = cargar_actividades(spark, ruta_actividades)

    df_datos = unir_datos(df_ciclistas, df_rutas, df_actividades)
    df_personas = total_km_por_persona(df_datos)

    mostrar_ranking(f"Top {TOP_N} por provincia según total de km",
                    top_por_total_km(df_personas, TOP_N))
    mostrar_ranking(f"Top {TOP_N} por provincia según promedio diario de km",
                    top_por_promedio_km(df_personas, TOP_N))

    spark.stop()


if __name__ == "__main__":
    main()