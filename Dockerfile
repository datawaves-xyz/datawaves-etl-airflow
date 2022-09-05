# Top level build args
ARG build_for=linux/amd64

FROM --platform=$build_for 149671120510.dkr.ecr.ap-northeast-1.amazonaws.com/airflow-spark:2.3.2-3.1.2

USER airflow

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
