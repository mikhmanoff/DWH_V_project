FROM apache/airflow:2.8.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    freetds-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    clickhouse-driver==0.2.6 \
    psycopg2-binary==2.9.9 \
    pymssql==2.2.11 \
    pymysql==1.1.0 \
    PyYAML==6.0.1 \
    pyarrow==15.0.0