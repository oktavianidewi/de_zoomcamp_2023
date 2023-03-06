FROM cluster-base

# -- Layer: JupyterLab

ARG spark_version=3.0.0
ARG jupyterlab_version=2.1.5

RUN apt-get update -y && \
    apt-get install -y python3-pip && \
    pip3 install wget pyspark==${spark_version} jupyterlab==${jupyterlab_version}

# pip3 install wget --no-verbose https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar -O jars/gcs-connector-hadoop3-latest.jar

COPY requirements.txt .
RUN pip3 install -r requirements.txt

# -- Runtime

EXPOSE 8888 4040
WORKDIR ${SHARED_WORKSPACE}

CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=