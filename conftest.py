import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark_session():
    s = SparkSession.builder.appName('pytest-local-spark').master('local').getOrCreate()
    yield s
    s.stop()