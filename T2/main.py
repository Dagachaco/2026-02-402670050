import sys

from session import get_spark
from etl import (leerCrossref, armarParquet, escribirParquet, leerParquet,
                 articulosPorMesAnio, articulosPorGrupo, areasPorPersona,
                 referenciasPorPersona, personaMasReferencia, percentil,
                 guardar)


def main(rutas, salida="output"):
    if not rutas:
        print("Uso: spark-submit programaestudiante.py <archivos .json>")
        return 1

    spark = get_spark()
    try:
        crudo = leerCrossref(spark, rutas)
        escribirParquet(armarParquet(crudo), f"{salida}/crossref-parquet")

        
        df = leerParquet(spark, f"{salida}/crossref-parquet")

        guardar(articulosPorMesAnio(df), f"{salida}/articles_month_year", "csv")
        guardar(articulosPorGrupo(df), f"{salida}/research_group", "csv")
        guardar(areasPorPersona(df), f"{salida}/research_areas_per_person", "json")

    
        totales = referenciasPorPersona(df).cache()
        guardar(personaMasReferencia(totales),
                f"{salida}/person_most_references", "json")
        for p in (25, 50, 75):
            guardar(percentil(totales, p / 100), f"{salida}/percentil_{p}", "csv")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))