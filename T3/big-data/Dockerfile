FROM eclipse-temurin:17-jdk-jammy

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV SPARK_HOME=/opt/venv/lib/python3.10/site-packages/pyspark

ENV ELASTICSEARCH_SPARK_VERSION=8.14.3
ENV POSTGRESQL_JDBC_VERSION=42.7.3

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      bash nano curl wget build-essential postgresql-client \
      python3 python3-dev python3-pip python3-venv \
      libpq-dev libffi-dev libopenblas-dev zlib1g-dev \
      libjpeg-dev libzmq3-dev && \
    python3 -m venv $VIRTUAL_ENV && \
    pip install --upgrade pip setuptools wheel && \
    pip install \
      numpy pandas matplotlib seaborn pyspark==3.5.1 pytest notebook findspark \
      psycopg2-binary sqlalchemy elasticsearch==8.14.0 pyarrow scikit-learn scipy tqdm && \
    mkdir -p $SPARK_HOME/jars && \
    wget -O $SPARK_HOME/jars/elasticsearch-spark-30_2.12-${ELASTICSEARCH_SPARK_VERSION}.jar \
      https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/${ELASTICSEARCH_SPARK_VERSION}/elasticsearch-spark-30_2.12-${ELASTICSEARCH_SPARK_VERSION}.jar && \
    wget -O $SPARK_HOME/jars/postgresql-${POSTGRESQL_JDBC_VERSION}.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRESQL_JDBC_VERSION}/postgresql-${POSTGRESQL_JDBC_VERSION}.jar && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY . /src