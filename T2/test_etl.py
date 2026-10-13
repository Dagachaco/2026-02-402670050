import glob
import pytest
import json
from pyspark.sql.functions import col
import main as programa
from etl import (nombreCompleto, leerCrossref, armarParquet, escribirParquet,
                 leerParquet, articulosPorMesAnio, articulosPorGrupo,
                 areasPorPersona, referenciasPorPersona,
                 personaMasReferencia, percentil, guardar)

ESQUEMA = ("doi string, createdDate string, groupTitle string, "
           "author array<struct<given:string,family:string>>, "
           "reference array<struct<DOI:string>>")


def crear_df(spark, filas):
    return spark.createDataFrame(filas, ESQUEMA)

def test_mes_anio_agrupa_ordena_y_respeta_utc(spark):
    df = crear_df(spark, [
        ("a1", "2021-01-10T10:00:00Z", "G", None, None),
        ("a2", "2020-12-31T23:30:00Z", "G", None, None),
        ("a3", "2020-12-01T00:00:00Z", "G", None, None),
    ])
    res = [tuple(r) for r in articulosPorMesAnio(df).collect()]
    assert res == [(2020, 12, 2), (2021, 1, 1)]


def test_referencias_solo_cuentan_las_que_tienen_doi(spark):
    df = crear_df(spark, [
        ("a1", "2020-03-15T10:00:00Z", "G",
         [("Ana", "Pérez"), ("Luis", "Mora")],
         [("10.1/x",), ("",), (None,)]),
    ])
    res = {r["full_name"]: r["total_references"]
           for r in referenciasPorPersona(df).collect()}
    assert res == {"Ana Pérez": 1, "Luis Mora": 1}


def test_referencias_articulo_sin_doi_no_crea_personas(spark):
    df = crear_df(spark, [
        ("a1", "2020-03-15T10:00:00Z", "G", [("Ana", "Pérez")], [(None,)]),
        ("a2", "2020-03-16T10:00:00Z", "G", [("Luis", "Mora")], None),
    ])
    assert referenciasPorPersona(df).count() == 0


def test_guardar_csv_un_solo_archivo_con_encabezado(spark, tmp_path):
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "n int, s string")
    ruta = str(tmp_path / "salida")
    guardar(df, ruta, "csv")
    partes = glob.glob(f"{ruta}/part-*.csv")
    assert len(partes) == 1
    with open(partes[0], encoding="utf-8") as f:
        assert f.readline().strip() == "n,s"


def escribir_json(ruta, doi="10.1/a", created="2020-03-15T10:00:00Z",
                  group="Grupo A", autores=None, referencias=None):
    mensaje = {
        "DOI": doi,
        "created": {"date-time": created},
        "group-title": group,
        "prefix": "10.1",
        "subtype": "preprint",
        "author": autores if autores is not None else [],
        "reference": referencias if referencias is not None else [],
    }
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump({"status": "ok", "message": mensaje}, f, indent=2)
    return str(ruta)


def leer_json_lines(carpeta):
    partes = glob.glob(f"{carpeta}/part-*.json")
    assert len(partes) == 1
    with open(partes[0], encoding="utf-8") as f:
        return [json.loads(linea) for linea in f if linea.strip()]



def test_nombreCompleto(spark):
    df = spark.createDataFrame([
        (("Ana", "Pérez"),),        
        (("Ana", None),),           
        ((None, "Pérez"),),         
        (("  Ana ", " Pérez  "),),  
        (("", ""),),                
        (("   ", "  "),),           
        ((None, None),),            
    ], "a struct<given:string,family:string>")
    res = [r[0] for r in df.select(nombreCompleto(col("a"))).collect()]
    assert res == ["Ana Pérez", "Ana", "Pérez", "Ana   Pérez",
                   "unknown", "unknown", "unknown"]



def test_leerCrossref(spark, tmp_path):
    ruta = escribir_json(tmp_path / "a.json", doi="10.1/a", group="G1")
    df = leerCrossref(spark, [ruta])
    fila = df.select("message.DOI", "message.`group-title`",
                     "message.created.`date-time`").first()
    assert tuple(fila) == ("10.1/a", "G1", "2020-03-15T10:00:00Z")


def test_leerCrossref_varios_archivos(spark, tmp_path):
    rutas = [escribir_json(tmp_path / f"{i}.json", doi=f"10.1/{i}")
             for i in range(3)]
    assert leerCrossref(spark, rutas).count() == 3
    assert leerCrossref(spark, str(tmp_path / "*.json")).count() == 3


def test_leerCrossref_campos_faltantes_son_null(spark, tmp_path):
    ruta = tmp_path / "min.json"
    ruta.write_text(json.dumps({"message": {"DOI": "10.1/z"}}), encoding="utf-8")
    fila = leerCrossref(spark, [str(ruta)]).select(
        "message.DOI", "message.`group-title`", "message.author",
        "message.reference").first()
    assert fila[0] == "10.1/z"
    assert fila[1] is None and fila[2] is None and fila[3] is None


COLUMNAS_PARQUET = ["doi", "author", "inst", "groupTitle", "createdDate",
                    "prefix", "reference", "link", "subtype"]


def test_armarParquet(spark, tmp_path):
    ruta = escribir_json(tmp_path / "a.json", doi="10.1/a", group="G1",
                         autores=[{"given": "Ana", "family": "Pérez"}],
                         referencias=[{"DOI": "10.2/x"}])
    res = armarParquet(leerCrossref(spark, [ruta]))
    assert res.columns == COLUMNAS_PARQUET
    fila = res.first()
    assert fila["doi"] == "10.1/a"
    assert fila["groupTitle"] == "G1"
    assert fila["createdDate"] == "2020-03-15T10:00:00Z"
    assert fila["prefix"] == "10.1"
    assert fila["subtype"] == "preprint"
    assert fila["author"][0]["given"] == "Ana"
    assert fila["reference"][0]["DOI"] == "10.2/x"


def test_armarParquet_no_pierde_filas(spark, tmp_path):
    rutas = [escribir_json(tmp_path / f"{i}.json", doi=f"10.1/{i}")
             for i in range(4)]
    assert armarParquet(leerCrossref(spark, rutas)).count() == 4



def test_escribirParque(spark, tmp_path):
    ruta = str(tmp_path / "pq")
    df = armarParquet(leerCrossref(
        spark, [escribir_json(tmp_path / "a.json")]))
    escribirParquet(df, ruta)
    assert len(glob.glob(f"{ruta}/*.parquet")) >= 1


def test_escribirParque_sobrescribe(spark, tmp_path):
    ruta = str(tmp_path / "pq")
    df = armarParquet(leerCrossref(
        spark, [escribir_json(tmp_path / "a.json")]))
    escribirParquet(df, ruta)
    escribirParquet(df, ruta)
    assert leerParquet(spark, ruta).count() == 1


def test_leerParquet(spark, tmp_path):
    ruta = str(tmp_path / "pq")
    df = armarParquet(leerCrossref(spark, [escribir_json(
        tmp_path / "a.json", doi="10.1/a",
        autores=[{"given": "Ana", "family": "Pérez"}])]))
    escribirParquet(df, ruta)
    leido = leerParquet(spark, ruta)
    assert leido.columns == COLUMNAS_PARQUET
    assert leido.schema == df.schema
    assert leido.first()["doi"] == "10.1/a"



def test_articulosPorMesAnio(spark):
    df = crear_df(spark, [
        ("a1", "2021-02-01T00:00:00Z", "G", None, None),
        ("a2", "2020-11-05T08:00:00Z", "G", None, None),
        ("a3", "2020-11-20T08:00:00Z", "G", None, None),
        ("a4", "2020-03-01T08:00:00Z", "G", None, None),
    ])
    res = articulosPorMesAnio(df)
    assert res.columns == ["created_year", "created_month", "total_articles"]
    assert [tuple(r) for r in res.collect()] == [
        (2020, 3, 1), (2020, 11, 2), (2021, 2, 1)]


def test_articulosPorMesAnio_vacio(spark):
    df = crear_df(spark, [])
    assert articulosPorMesAnio(df).count() == 0


def test_articulosPorGrupo(spark):
    df = crear_df(spark, [
        ("a1", "2020-03-15T10:00:00Z", "G1", None, None),
        ("a2", "2020-03-15T10:00:00Z", "G1", None, None),
        ("a3", "2020-03-15T10:00:00Z", "G2", None, None),
    ])
    res = {r["group_title"]: r["total"] for r in articulosPorGrupo(df).collect()}
    assert res == {"G1": 2, "G2": 1}


def test_articulosPorGrupo_no_cuenta_dois_repetidos(spark):
    df = crear_df(spark, [
        ("a1", "2020-03-15T10:00:00Z", "G1", None, None),
        ("a1", "2020-03-16T10:00:00Z", "G1", None, None),  # mismo DOI
        ("a2", "2020-03-17T10:00:00Z", "G1", None, None),
    ])
    res = {r["group_title"]: r["total"] for r in articulosPorGrupo(df).collect()}
    assert res == {"G1": 2}



def test_areasPorPersona(spark):
    df = crear_df(spark, [
        ("a1", "2020-03-15T10:00:00Z", "G1", [("Ana", "Pérez")], None),
        ("a2", "2020-03-15T10:00:00Z", "G2", [("Ana", "Pérez")], None),
        ("a3", "2020-03-15T10:00:00Z", "G1", [("Ana", "Pérez"), ("Luis", "Mora")], None),
    ])
    res = {r["full_name"]: (sorted(r["research_areas"]), r["total"])
           for r in areasPorPersona(df).collect()}
    assert res == {"Ana Pérez": (["G1", "G2"], 2), "Luis Mora": (["G1"], 1)}


def test_areasPorPersona_ordena_por_total_desc(spark):
    df = crear_df(spark, [
        ("a1", "2020-03-15T10:00:00Z", "G1", [("Luis", "Mora")], None),
        ("a2", "2020-03-15T10:00:00Z", "G1", [("Ana", "Pérez")], None),
        ("a3", "2020-03-15T10:00:00Z", "G2", [("Ana", "Pérez")], None),
        ("a4", "2020-03-15T10:00:00Z", "G3", [("Ana", "Pérez")], None),
    ])
    totales = [r["total"] for r in areasPorPersona(df).collect()]
    assert totales == [3, 1]
    assert areasPorPersona(df).columns == ["full_name", "research_areas", "total"]


def test_areasPorPersona_nombre_vacio_es_unknown(spark):
    df = crear_df(spark, [
        ("a1", "2020-03-15T10:00:00Z", "G1", [("", ""), ("  ", None)], None),
        ("a2", "2020-03-15T10:00:00Z", "G2", [(None, None)], None),
    ])
    res = areasPorPersona(df).collect()
    assert len(res) == 1
    assert res[0]["full_name"] == "unknown"
    assert sorted(res[0]["research_areas"]) == ["G1", "G2"]


def test_areasPorPersona_articulo_sin_autores(spark):
    df = crear_df(spark, [("a1", "2020-03-15T10:00:00Z", "G1", None, None)])
    assert areasPorPersona(df).count() == 0



def test_referenciasPorPersona(spark):
    df = crear_df(spark, [
        ("a1", "2020-03-15T10:00:00Z", "G", [("Ana", "Pérez")],
         [("10.1/x",), ("10.1/y",)]),
        ("a2", "2020-03-16T10:00:00Z", "G", [("Ana", "Pérez"), ("Luis", "Mora")],
         [("10.1/z",), ("  ",)]),
    ])
    res = {r["full_name"]: r["total_references"]
           for r in referenciasPorPersona(df).collect()}
    assert res == {"Ana Pérez": 3, "Luis Mora": 1}


def test_referenciasPorPersona_excluye_autores_sin_nombre(spark):
    df = crear_df(spark, [
        ("a1", "2020-03-15T10:00:00Z", "G",
         [("", ""), ("Ana", "Pérez")], [("10.1/x",)]),
    ])
    res = {r["full_name"]: r["total_references"]
           for r in referenciasPorPersona(df).collect()}
    assert res == {"Ana Pérez": 1}



def _totales(spark, datos):
    return spark.createDataFrame(datos, "full_name string, total_references long")


def test_referenciasPorPerson(spark):
    totales = _totales(spark, [("Ana", 5), ("Luis", 9), ("Eva", 2)])
    res = personaMasReferencia(totales).collect()
    assert len(res) == 1
    assert (res[0]["full_name"], res[0]["total_references"]) == ("Luis", 9)


def test_personaMasReferencia_empate_es_deterministico(spark):
    totales = _totales(spark, [("Zoe", 9), ("Ana", 9), ("Eva", 2)])
    res = personaMasReferencia(totales).collect()
    assert len(res) == 1 and res[0]["full_name"] == "Ana"


def test_personaMasReferencia_vacio(spark):
    assert personaMasReferencia(_totales(spark, [])).count() == 0



def _valor(df):
    filas = df.collect()
    assert len(filas) == 1
    return filas[0]["total_references"]


def test_percentil(spark):
    totales = _totales(spark, [(f"p{i}", i) for i in range(1, 101)])  
    assert _valor(percentil(totales, 0.25)) == 25
    assert _valor(percentil(totales, 0.50)) == 50
    assert _valor(percentil(totales, 0.75)) == 75


def test_percentil_extremos(spark):
    totales = _totales(spark, [("a", 4), ("b", 10), ("c", 7)])
    assert _valor(percentil(totales, 0)) == 4
    assert _valor(percentil(totales, 1)) == 10


def test_percentil_un_solo_valor(spark):
    totales = _totales(spark, [("a", 42)])
    assert _valor(percentil(totales, 0.25)) == 42
    assert _valor(percentil(totales, 0.75)) == 42


def test_percentil_no_depende_del_orden_de_entrada(spark):
    datos = [(f"p{i}", i) for i in range(1, 101)]
    revertido = _totales(spark, list(reversed(datos)))
    assert _valor(percentil(revertido, 0.50)) == 50


@pytest.mark.parametrize("p", [-0.1, 1.5, 25, 100])
def test_percentil_p_invalido(spark, p):
    with pytest.raises(ValueError):
        percentil(_totales(spark, [("a", 1)]), p)


def test_percentil_vacio_devuelve_null(spark):
    assert _valor(percentil(_totales(spark, []), 0.5)) is None


def test_guardar(spark, tmp_path):
    df = spark.createDataFrame([(1, "a"), (2, "b")], "n int, s string")
    ruta = str(tmp_path / "salida")
    guardar(df, ruta, "json")
    filas = leer_json_lines(ruta)
    assert sorted(filas, key=lambda d: d["n"]) == [
        {"n": 1, "s": "a"}, {"n": 2, "s": "b"}]


def test_guardar_formato_no_soportado(spark, tmp_path):
    df = spark.createDataFrame([(1,)], "n int")
    with pytest.raises(ValueError):
        guardar(df, str(tmp_path / "x"), "xml")


def test_guardar_sobrescribe(spark, tmp_path):
    ruta = str(tmp_path / "salida")
    guardar(spark.createDataFrame([(1,), (2,)], "n int"), ruta, "csv")
    guardar(spark.createDataFrame([(9,)], "n int"), ruta, "csv")
    partes = glob.glob(f"{ruta}/part-*.csv")
    assert len(partes) == 1
    with open(partes[0], encoding="utf-8") as f:
        assert f.read().split() == ["n", "9"]


def test_guardar_json_un_solo_archivo_aunque_haya_varias_particiones(spark, tmp_path):
    df = spark.range(0, 100).repartition(8)
    ruta = str(tmp_path / "salida")
    guardar(df, ruta, "json")
    assert len(glob.glob(f"{ruta}/part-*.json")) == 1


def test_main_sin_archivos_devuelve_1():
    assert programa.main([]) == 1

