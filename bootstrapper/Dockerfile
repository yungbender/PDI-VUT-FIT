FROM bitnami/spark:3

RUN mkdir /tmp/temp-jars
WORKDIR /tmp/temp-jars
RUN curl https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar --output commons-pool2-2.11.1.jar
RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.2.0/spark-token-provider-kafka-0-10_2.12-3.2.0.jar --output spark-token-provider-kafka-0-10_2.12-3.2.0.jar
RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.0/spark-sql-kafka-0-10_2.12-3.2.0.jar --output spark-sql-kafka-0-10_2.12-3.2.0.jar
RUN curl https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.0.0/kafka-clients-3.0.0.jar --output kafka-clients-3.0.0.jar
RUN mv *.jar /opt/bitnami/spark/jars/

WORKDIR /bootstrapper
COPY "./spark/spark.py" "."
COPY "./bootstrapper/requirements.txt" "."
COPY "./bootstrapper/bootstrapper.py" "."

USER root
RUN pip3 install -r requirements.txt
