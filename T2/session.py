from pyspark.sql import SparkSession


def get_spark(master=None, app_name="Tarea2-Crossref"):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()