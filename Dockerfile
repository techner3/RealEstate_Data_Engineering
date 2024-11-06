FROM apache/airflow:2.10.2-python3.12
USER root
RUN apt-get update && \
    apt-get install -y git && \
    apt-get install -y default-jre && \
    apt-get clean
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:${PATH}"
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt &&\
    pip install git+https://github.com/dpkp/kafka-python.git
