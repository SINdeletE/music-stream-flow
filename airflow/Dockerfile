ARG AIRFLOW_IMAGE_NAME

FROM ${AIRFLOW_IMAGE_NAME}
ADD requirements.txt .

ARG AIRFLOW_VERSION
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt