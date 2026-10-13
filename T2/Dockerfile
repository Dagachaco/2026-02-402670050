FROM eclipse-temurin:17-jdk-alpine

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN apk add bash && \
  apk add nano && \
  apk add build-base && \
  apk add postgresql-client && \
  apk add python3 && \
  apk add python3-dev && \
  apk add libffi-dev && \
  apk add openblas-dev && \
  apk add zlib-dev && \
  apk add jpeg-dev && \
  apk add zeromq-dev && \
  apk add --no-cache py3-pip && \
  apk add --no-cache py3-virtualenv && \
  python3 -m venv $VIRTUAL_ENV && \
  pip install --upgrade pip setuptools wheel && \
  pip install numpy && \
  pip install matplotlib && \
  pip install seaborn && \
  pip install pyspark && \
  pip install pytest && \
  pip install notebook && \
  pip install findspark

WORKDIR /src

COPY . /src
