FROM bitnami/spark:3

# download jars for spark
RUN mkdir /tmp/temp-jars
WORKDIR /tmp/temp-jars
RUN curl https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar --output commons-pool2-2.11.1.jar
RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.2.0/spark-token-provider-kafka-0-10_2.12-3.2.0.jar --output spark-token-provider-kafka-0-10_2.12-3.2.0.jar
RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.0/spark-sql-kafka-0-10_2.12-3.2.0.jar --output spark-sql-kafka-0-10_2.12-3.2.0.jar
RUN curl https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.0.0/kafka-clients-3.0.0.jar --output kafka-clients-3.0.0.jar
RUN mv *.jar /opt/bitnami/spark/jars/

# copy source files
WORKDIR /bootstrapper
COPY "./spark/spark.py" "."
COPY "./bootstrapper/requirements.txt" "."
COPY "./bootstrapper/bootstrapper.py" "."
COPY "./spark/requirements.txt" "./job-requirements.txt"

# install requirements
USER root
RUN pip3 install -r requirements.txt
RUN pip3 install -r job-requirements.txt

# install zip for pyspark job dependencies
WORKDIR /tmp
RUN curl http://archive.ubuntu.com/ubuntu/pool/main/z/zip/zip_3.0-11build1_amd64.deb --output zip.deb
RUN apt install ./zip.deb

# create zip with pyspark dependencies
WORKDIR /bootstrapper
RUN pip3 install -r job-requirements.txt -t job-requirements
RUN cd job-requirements && \
    zip -r ../job-requirements.zip . && \
    cd .. && \
    rm -rf job-requirements.txt job-requirements
