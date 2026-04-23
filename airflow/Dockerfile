FROM apache/airflow:2.8.1

# Install Spark provider + PySpark so SparkSubmitOperator has spark-submit available
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
