import datetime
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DoubleType, DateType
)

from functions.functions import (
    CICLISTA_SCHEMA, RUTA_SCHEMA, ACTIVIDAD_SCHEMA,
    unir_datos,
    total_km_por_persona, total_km_por_provincia, total_km_por_dia,
    top_por_total_km, top_por_promedio_km
)

D = datetime.date

# Esquema del resultado de unir_datos
DATOS_SCHEMA = StructType([
    StructField("cedula", LongType(), True),
    StructField("nombre", StringType(), True),
    StructField("provincia", StringType(), True),
    StructField("codigo_ruta", LongType(), True),
    StructField("fecha", DateType(), True),
    StructField("nombre_ruta", StringType(), True),
    StructField("kilometros", DoubleType(), True),
])

# Esquema del resultado de total_km_por_persona
PERSONA_SCHEMA = StructType([
    StructField("cedula", LongType(), True),
    StructField("nombre", StringType(), True),
    StructField("provincia", StringType(), True),
    StructField("total_km", DoubleType(), True),
    StructField("dias_activos", LongType(), True),
    StructField("promedio_diario_km", DoubleType(), True),
])


#Funciones de apoyo

def datos(spark, filas):
    return spark.createDataFrame(filas, DATOS_SCHEMA)


def personas(spark, filas):
    return spark.createDataFrame(filas, PERSONA_SCHEMA)


def unir(spark, ciclistas, rutas, actividades):
    return unir_datos(
        spark.createDataFrame(ciclistas, CICLISTA_SCHEMA),
        spark.createDataFrame(rutas, RUTA_SCHEMA),
        spark.createDataFrame(actividades, ACTIVIDAD_SCHEMA),
    )


def fila_de(df, cedula):
    return df.filter(F.col("cedula") == cedula).collect()[0]


def cedulas_top(df, provincia):
    return {r["cedula"] for r in df.filter(F.col("provincia") == provincia).collect()}


def cedulas_en_orden(df, provincia):
    filas = df.filter(F.col("provincia") == provincia).orderBy("posicion").collect()
    return [r["cedula"] for r in filas]


CICLISTAS = [(1, "Ana", "San José"), (2, "Luis", "Cartago")]
RUTAS = [(10, "Ruta A", 20.0), (20, "Ruta B", 35.5)]


def tabla_top(spark):
    return personas(spark, [
        (1, "A1", "San José", 100.0, 5, 20.0),
        (2, "A2", "San José", 80.0, 4, 20.0),
        (3, "A3", "San José", 60.0, 3, 20.0),
        (4, "A4", "San José", 40.0, 2, 20.0),
        (5, "B1", "Cartago", 500.0, 10, 50.0),
        (6, "B2", "Cartago", 10.0, 1, 10.0),
    ])


# Pruebas

def test_unir_caso_basico(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(1, "Ana", "San José")], CICLISTA_SCHEMA
    )
    df_rutas = spark_session.createDataFrame(
        [(100, "Ruta A", 25.5)], RUTA_SCHEMA
    )
    df_actividades = spark_session.createDataFrame(
        [(100, 1, D(2026, 5, 1))], ACTIVIDAD_SCHEMA
    )

    resultado = unir_datos(df_ciclistas, df_rutas, df_actividades).collect()

    assert len(resultado) == 1
    assert resultado[0]["kilometros"] == 25.5
    assert resultado[0]["provincia"] == "San José"


def test_ciclista_sin_actividades_aparece_en_la_union(spark_session):
    r = unir(spark_session, CICLISTAS, RUTAS, [(10, 1, D(2026, 1, 1))])

    assert r.count() == 2
    luis = fila_de(r, 2)
    assert luis["fecha"] is None
    assert luis["kilometros"] is None


def test_union_misma_ruta_mismo_dia_no_elimina_registros(spark_session):
    acts = [(10, 1, D(2026, 1, 1)), (10, 1, D(2026, 1, 1))]
    r = unir(spark_session, CICLISTAS, RUTAS, acts)

    assert r.filter("cedula = 1").count() == 2


def test_union_ruta_inexistente_conserva_la_actividad(spark_session):
    r = unir(spark_session, CICLISTAS, RUTAS, [(99, 1, D(2026, 1, 1))])

    ana = fila_de(r, 1)
    assert ana["fecha"] == D(2026, 1, 1)
    assert ana["kilometros"] is None



def test_dias_activos_cuenta_dias_distintos_no_actividades(spark_session):
    df = datos(spark_session, [
        (1, "Ana", "San José", 100, D(2026, 1, 1), "Ruta A", 10.0),
        (1, "Ana", "San José", 101, D(2026, 1, 1), "Ruta B", 20.0),  # mismo día
        (1, "Ana", "San José", 102, D(2026, 1, 2), "Ruta C", 30.0),
    ])

    fila = fila_de(total_km_por_persona(df), 1)

    assert fila["total_km"] == pytest.approx(60.0)
    assert fila["dias_activos"] == 2
    assert fila["promedio_diario_km"] == pytest.approx(30.0)


def test_persona_misma_ruta_mismo_dia_suma_km_pero_cuenta_un_dia(spark_session):
    df = datos(spark_session, [
        (1, "Ana", "San José", 10, D(2026, 1, 1), "R", 10.0),
        (1, "Ana", "San José", 10, D(2026, 1, 1), "R", 10.0),
    ])

    f = fila_de(total_km_por_persona(df), 1)

    assert f["total_km"] == pytest.approx(20.0)
    assert f["dias_activos"] == 1
    assert f["promedio_diario_km"] == pytest.approx(20.0)


def test_persona_sin_actividades_queda_en_cero(spark_session):
    df = datos(spark_session, [(2, "Luis", "Cartago", None, None, None, None)])

    f = fila_de(total_km_por_persona(df), 2)

    assert f["total_km"] == pytest.approx(0.0)
    assert f["dias_activos"] == 0
    assert f["promedio_diario_km"] == pytest.approx(0.0)


def test_persona_con_ruta_inexistente_cuenta_dia_pero_no_km(spark_session):
    df = datos(spark_session, [(1, "Ana", "San José", 99, D(2026, 1, 1), None, None)])

    f = fila_de(total_km_por_persona(df), 1)

    assert f["total_km"] == pytest.approx(0.0)
    assert f["dias_activos"] == 1
    assert f["promedio_diario_km"] == pytest.approx(0.0)


def test_persona_varias_personas_no_se_mezclan(spark_session):
    df = datos(spark_session, [
        (1, "Ana", "San José", 10, D(2026, 1, 1), "R", 10.0),
        (2, "Luis", "Cartago", 10, D(2026, 1, 1), "R", 99.0),
    ])

    r = total_km_por_persona(df)

    assert fila_de(r, 1)["total_km"] == pytest.approx(10.0)
    assert fila_de(r, 2)["total_km"] == pytest.approx(99.0)


def test_persona_dataframe_vacio(spark_session):
    assert total_km_por_persona(datos(spark_session, [])).count() == 0


def totales_prov(df):
    return {r["provincia"]: r["total_km"] for r in df.collect()}


def test_provincia_varias_personas_misma_provincia(spark_session):
    df = datos(spark_session, [
        (1, "Ana", "San José", 10, D(2026, 1, 1), "R", 10.0),
        (2, "Luis", "San José", 10, D(2026, 1, 1), "R", 5.0),
        (2, "Luis", "San José", 10, D(2026, 1, 2), "R", 7.0),
    ])

    assert totales_prov(total_km_por_provincia(df))["San José"] == pytest.approx(22.0)


def test_provincia_personas_de_provincias_distintas(spark_session):
    df = datos(spark_session, [
        (1, "Ana", "San José", 10, D(2026, 1, 1), "R", 10.0),
        (2, "Luis", "Cartago", 10, D(2026, 1, 1), "R", 5.0),
    ])

    t = totales_prov(total_km_por_provincia(df))

    assert t == {"San José": pytest.approx(10.0), "Cartago": pytest.approx(5.0)}


def test_provincia_sin_actividades_tiene_cero(spark_session):
    df = datos(spark_session, [(1, "Ana", "Heredia", None, None, None, None)])

    assert totales_prov(total_km_por_provincia(df))["Heredia"] == pytest.approx(0.0)


def totales_dia(df):
    return {r["fecha"]: r["total_km"] for r in df.collect()}


def test_dia_varias_personas_mismo_dia(spark_session):
    df = datos(spark_session, [
        (1, "Ana", "San José", 10, D(2026, 1, 1), "R", 10.0),
        (2, "Luis", "Cartago", 10, D(2026, 1, 1), "R", 30.0),
    ])

    assert totales_dia(total_km_por_dia(df))[D(2026, 1, 1)] == pytest.approx(40.0)


def test_dia_actividades_en_dias_distintos(spark_session):
    df = datos(spark_session, [
        (1, "Ana", "San José", 10, D(2026, 1, 1), "R", 10.0),
        (1, "Ana", "San José", 10, D(2026, 1, 2), "R", 20.0),
    ])

    t = totales_dia(total_km_por_dia(df))

    assert t[D(2026, 1, 1)] == pytest.approx(10.0)
    assert t[D(2026, 1, 2)] == pytest.approx(20.0)


def test_dia_fechas_nulas_no_aparecen(spark_session):
    df = datos(spark_session, [
        (1, "Ana", "San José", 10, D(2026, 1, 1), "R", 10.0),
        (2, "Luis", "Cartago", None, None, None, None),  # sin actividades
    ])

    t = totales_dia(total_km_por_dia(df))

    assert None not in t
    assert len(t) == 1



def test_top_total_provincia_con_mas_de_n(spark_session):
    r = top_por_total_km(tabla_top(spark_session), 2)

    assert cedulas_top(r, "San José") == {1, 2}


def test_top_total_provincia_con_menos_de_n(spark_session):
    r = top_por_total_km(tabla_top(spark_session), 3)

    assert cedulas_top(r, "Cartago") == {5, 6}  # solo hay 2


def test_top_total_es_por_provincia_no_global(spark_session):
    r = top_por_total_km(tabla_top(spark_session), 2)
    assert 6 in cedulas_top(r, "Cartago")
    assert r.count() == 4


def test_top_total_orden_y_posicion(spark_session):
    r = top_por_total_km(tabla_top(spark_session), 3)

    assert cedulas_en_orden(r, "San José") == [1, 2, 3]
    assert cedulas_en_orden(r, "Cartago") == [5, 6]
    posiciones_cartago = [
        f["posicion"]
        for f in r.filter("provincia = 'Cartago'").orderBy("posicion").collect()
    ]
    assert posiciones_cartago == [1, 2]  


def test_top_total_empate_se_desempata_por_cedula(spark_session):
    df = personas(spark_session, [
        (1, "A1", "San José", 100.0, 5, 20.0),
        (3, "A3", "San José", 50.0, 5, 10.0),   
        (2, "A2", "San José", 50.0, 5, 10.0),
    ])

    r = top_por_total_km(df, 2)

    assert cedulas_en_orden(r, "San José") == [1, 2] 


def test_top_total_excluye_ciclistas_sin_actividades(spark_session):
    df = personas(spark_session, [
        (1, "A1", "San José", 10.0, 1, 10.0),
        (2, "A2", "San José", 0.0, 0, 0.0),
    ])

    assert cedulas_top(top_por_total_km(df, 5), "San José") == {1}


@pytest.mark.parametrize("n", [0, -3])
def test_top_n_no_positivo_devuelve_vacio(spark_session, n):
    df = tabla_top(spark_session)

    assert top_por_total_km(df, n).count() == 0
    assert top_por_promedio_km(df, n).count() == 0


def test_top_total_dataframe_vacio(spark_session):
    assert top_por_total_km(personas(spark_session, []), 5).count() == 0

def test_top_promedio_distingue_total_de_promedio(spark_session):
    df = personas(spark_session, [
        (1, "A", "San José", 500.0, 10, 50.0),    
        (2, "B", "San José", 300.0, 2, 150.0),    
    ])

    assert cedulas_top(top_por_total_km(df, 1), "San José") == {1}
    assert cedulas_top(top_por_promedio_km(df, 1), "San José") == {2}


def test_top_promedio_orden_por_provincia(spark_session):
    df = personas(spark_session, [
        (1, "A", "San José", 90.0, 9, 10.0),
        (2, "B", "San José", 60.0, 2, 30.0),
        (3, "C", "San José", 45.0, 3, 15.0),
        (4, "D", "Cartago", 20.0, 2, 10.0),
    ])

    r = top_por_promedio_km(df, 3)

    assert cedulas_en_orden(r, "San José") == [2, 3, 1]
    assert cedulas_en_orden(r, "Cartago") == [4]


def test_top_promedio_excluye_ciclistas_sin_actividades(spark_session):
    df = personas(spark_session, [
        (1, "A1", "San José", 10.0, 1, 10.0),
        (2, "A2", "San José", 0.0, 0, 0.0),
    ])

    assert cedulas_top(top_por_promedio_km(df, 5), "San José") == {1}


def test_top_promedio_dataframe_vacio(spark_session):
    assert top_por_promedio_km(personas(spark_session, []), 5).count() == 0

def test_union_asocia_ciclista_y_ruta_correctos(spark_session):
    acts = [(10, 1, D(2026, 1, 1)), (20, 2, D(2026, 1, 2))]
    r = unir(spark_session, CICLISTAS, RUTAS, acts)

    ana, luis = fila_de(r, 1), fila_de(r, 2)
    assert (ana["nombre_ruta"], ana["kilometros"], ana["fecha"]) == ("Ruta A", 20.0, D(2026, 1, 1))
    assert (luis["nombre_ruta"], luis["kilometros"], luis["fecha"]) == ("Ruta B", 35.5, D(2026, 1, 2))


def test_union_ciclista_con_varias_actividades(spark_session):
    acts = [(10, 1, D(2026, 1, 1)), (20, 1, D(2026, 1, 2)), (10, 1, D(2026, 1, 3))]
    r = unir(spark_session, CICLISTAS, RUTAS, acts)

    filas_ana = r.filter("cedula = 1").collect()
    assert len(filas_ana) == 3
    assert {f["fecha"] for f in filas_ana} == {D(2026, 1, 1), D(2026, 1, 2), D(2026, 1, 3)}

def test_top_promedio_empate_se_desempata_por_cedula(spark_session):
    df = personas(spark_session, [
        (1, "A1", "San José", 100.0, 2, 50.0),
        (3, "A3", "San José", 60.0, 2, 30.0),   # empata con la cédula 2
        (2, "A2", "San José", 60.0, 2, 30.0),
    ])

    r = top_por_promedio_km(df, 2)

    assert cedulas_en_orden(r, "San José") == [1, 2]

def test_provincia_dataframe_vacio(spark_session):
    assert total_km_por_provincia(datos(spark_session, [])).count() == 0


def test_dia_dataframe_vacio(spark_session):
    assert total_km_por_dia(datos(spark_session, [])).count() == 0