FROM apache/airflow:latest-python3.9

COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt

COPY data /opt/data/