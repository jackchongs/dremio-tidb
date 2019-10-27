FROM dremio/dremio-oss:4.0.0
USER root
WORKDIR /tmp
COPY . .
RUN cp target/dremio-tidb*.jar /opt/dremio/jars && \
   cd /opt/dremio/jars && wget https://mirrors.huaweicloud.com/repository/maven/mysql/mysql-connector-java/5.1.48/mysql-connector-java-5.1.48.jar && \
   chown dremio *mysql-connector-java*.jar && rm -rf ~/.m2 && rm -rf /tmp/*
WORKDIR /opt/dremio
USER dremio