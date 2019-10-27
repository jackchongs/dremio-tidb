# dremio-tidb

这是一个ARP框架制作的社区版Tidb Dremio连接器

# Dremio介绍
Dremio是一个分布式的分析引擎，可一站式满足实时和海量数据离线即时分析

Dremio提供闪电般的快速查询速度和自助语义层，可直接针对您的数据湖存储和其他来源运行。
无需将数据移至专有数据仓库或创建多维数据集，聚合表和BI提取。对于数据架构师来说只是灵活性和控制，
对于数据使用者来说就是自助服务。

# 特点
1、完成数据类型的转换支持

2、实现将50+个函数进行push down

3、检验所有TPCH的push down


# 使用

**创建一个Tidb数据源**

① 使用JDBC URL

jdbc:tidb://<account_name>.snowflakecomputing.com/?param1=value&param2=value. 

② 使用账号密码

# 开发

**构建和安装**


① 在带有pom.xml文件的根目录中运行`mvn clean install -DskipTests`。如果要运行测试，请将JDBC jar和所需的环境变量一起添加到本地maven存储库中。

② 将生成的.jar文件放在目标文件夹中，并将其放在Dremio的<DREMIO_HOME> \ jars文件夹中

③ 重新启动Dremio

**构建docker镜像详细步骤**

`FROM dremio/dremio-oss:4.0.0`

`USER root`

`RUN wget http://apache.osuosl.org/maven/maven-3/3.6.1/binaries/apache-maven-3.6.1-bin.zip && \`
	
	`unzip apache-maven-3.6.1-bin.zip && \'
	
	git clone https://github.com/jackchongs/dremio-tidb.git && cd dremio-tidb && \
	
	export PATH=$PATH:/tmp/apache-maven-3.6.1/bin && \
	
	mvn clean install -DskipTests && \
	
	cp target/dremio-tidb*.jar /opt/dremio/jars && \
	
	cd /opt/dremio/jars && wget https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.9.1/snowflake-jdbc-3.9.1.jar && \
	chown dremio *snowflake*.jar && rm -rf ~/.m2 && rm -rf /tmp/*

`WORKDIR /opt/dremio`

`USER dremio`