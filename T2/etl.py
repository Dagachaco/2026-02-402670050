from pyspark.sql.functions import col, count, month, substring, to_date, year, countDistinct, collect_set, concat_ws, explode, size, trim, when, expr, lit
from pyspark.sql import functions as F

input_schema = """
message STRUCT<
  DOI: STRING,
  author: ARRAY<STRUCT<
    ORCID: STRING,
    affiliation: ARRAY<STRUCT<name: STRING>>,
    `authenticated-orcid`: BOOLEAN,
    family: STRING,
    given: STRING,
    name: STRING,
    sequence: STRING,
    suffix: STRING>>,
  institution: ARRAY<STRUCT<name: STRING>>,
  `group-title`: STRING,
  created: STRUCT<`date-time`: STRING>,
  prefix: STRING,
  reference: ARRAY<STRUCT<
    DOI: STRING,
    ISSN: STRING,
    `article-title`: STRING,
    author: STRING,
    `doi-asserted-by`: STRING,
    `first-page`: STRING,
    `issn-type`: STRING,
    issue: STRING,
    `journal-title`: STRING,
    key: STRING,
    unstructured: STRING,
    volume: STRING,
    `volume-title`: STRING,
    year: STRING>>,
  link: ARRAY<STRUCT<
    URL: STRING,
    `content-type`: STRING,
    `content-version`: STRING,
    `intended-application`: STRING>>,
  subtype: STRING>
"""


def leerCrossref(spark, paths):
    return (spark.read.option("multiLine", True).schema(input_schema).json(paths))

# Parquet

def armarParquet(df):
    return df.select(col("message.DOI").alias("doi"), col("message.author").alias("author"), col("message.institution").alias("inst"),
                     col("message.`group-title`").alias("groupTitle"), col("message.created.`date-time`").alias("createdDate"),
                     col("message.prefix").alias("prefix"), col("message.reference").alias("reference"), col("message.link").alias("link"),
                     col("message.subtype").alias("subtype"))  

def escribirParquet(df, ruta):
    df.write.mode("overwrite").parquet(ruta)


def leerParquet(spark, ruta):
    return spark.read.parquet(ruta)

def articulosPorMesAnio(df):
    fecha = to_date(substring(col("createdDate"), 1, 10))
    return (df.select(year(fecha).alias("created_year"), month(fecha).alias("created_month"))
            .groupBy("created_year", "created_month").agg(count("*").alias("total_articles"))
            .orderBy("created_year", "created_month"))

def articulosPorGrupo(df):
    return (df.groupBy(col("groupTitle").alias("group_title"))
              .agg(countDistinct("doi").alias("total")))

def areasPorPersona(df):
    personas = (df.select("groupTitle", explode("author").alias("a"))
                  .select(nombreCompleto(col("a")).alias("full_name"),"groupTitle"))
    return (personas.groupBy("full_name")
                    .agg(collect_set("groupTitle").alias("research_areas"))
                    .withColumn("total", size("research_areas"))
                    .orderBy(col("total").desc(), "full_name"))

def referenciasPorPersona(df):
    articulos = (df.withColumn("n_refs", when(col("reference").isNull(), lit(0))
                .otherwise(expr("size(filter(reference, " "r -> r.DOI is not null and trim(r.DOI) != ''))")))
                .filter(col("n_refs") > 0))
    
    personas = (articulos
        .select("n_refs", explode("author").alias("a"))
        .select(nombreCompleto(col("a")).alias("full_name"), "n_refs")
        .filter(col("full_name") != "unknown"))

    return (personas.groupBy("full_name")
                    .agg(F.sum("n_refs").alias("total_references")))

def personaMasReferencia(df):
    return (df.orderBy(col("total_references").desc(), "full_name").limit(1))

def percentil(df, p):
    if not 0 <= p <= 1:raise ValueError("p debe estar entre 0 y 1")
    return df.select(expr(f"percentile_approx(total_references, {p}, 10000000)").alias("total_references"))

def nombreCompleto(autor):
    nombre = F.trim(F.concat_ws(" ", autor["given"], autor["family"]))
    return F.when(nombre == "", F.lit("unknown")).otherwise(nombre)


def guardar(df, ruta, formato="csv"):
    escritor = df.coalesce(1).write.mode("overwrite")
    if formato == "csv": escritor.option("header", True).csv(ruta)
    elif formato == "json": escritor.json(ruta)
    else: raise ValueError(f"Formato no soportado: {formato}")