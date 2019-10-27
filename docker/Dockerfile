FROM dremio/dremio-oss:4.0.0
USER root
RUN wget http://apache.osuosl.org/maven/maven-3/3.6.1/binaries/apache-maven-3.6.1-bin.zip && \
   unzip apache-maven-3.6.1-bin.zip
RUN git clone https://github.com/jackchongs/dremio-tidb.git && cd dremio-tidb && \
   export PATH=$PATH:/tmp/apache-maven-3.6.1/bin && \
   mvn clean install -DskipTests && \
   cp target/dremio-tidb*.jar /opt/dremio/jars && \
   cd /opt/dremio/jars && wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.48/mysql-connector-java-5.1.48.jar && \
   chown dremio *mysql-connector-java*.jar && rm -rf ~/.m2 && rm -rf /tmp/*
WORKDIR /opt/dremio
USER dremio