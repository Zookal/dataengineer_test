ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.0.0-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

# pyarrow required for writing to_parquet() with Pandas
# minio required for communicating with MinIO
# geopandas & pygeos for mapping lat lon coordinates to NYC taxi zone ids
RUN pip install --user --no-cache-dir \
    pandas==1.1.4