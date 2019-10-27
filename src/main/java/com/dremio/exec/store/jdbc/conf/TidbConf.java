/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dremio.exec.store.jdbc.conf;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.sql.DataSource;
import org.apache.log4j.Logger;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcher;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin.Config;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.google.common.annotations.VisibleForTesting;
import io.protostuff.Tag;

/**
 * Configuration for Tidb.
 */
@SourceType(value = "TIDB", label = "Tidb")
public class TidbConf extends AbstractArpConf<TidbConf> {

  private static final String ARP_FILENAME = "arp/implementation/tidb-arp.yaml";
  private static final ArpDialect ARP_DIALECT =
      AbstractArpConf.loadArpFile(ARP_FILENAME, (TidbDialect::new));
  private static final String DRIVER = "com.mysql.jdbc.Driver";
  private static Logger logger = Logger.getLogger(TidbConf.class);

  /*
    TODO: check following
    The following block is required as Tidb reports integers as NUMBER(38,0).
   */
  static class TidbSchemaFetcher extends JdbcSchemaFetcher {

    public TidbSchemaFetcher(String name, DataSource dataSource, int timeout, Config config) {
      super(name, dataSource, timeout, config);
    }

    protected boolean usePrepareForColumnMetadata() {
      return true;
    }
  }

  static class TidbDialect extends ArpDialect {

    public TidbDialect(ArpYaml yaml) {
      super(yaml);
    }

    public JdbcSchemaFetcher getSchemaFetcher(String name, DataSource dataSource, int timeout,
        JdbcStoragePlugin.Config config) {
      return new TidbSchemaFetcher(name, dataSource, timeout, config);
    }
  }

  /*
     Check mysql JDBC connection docs for more details: https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference.html
   */
  @Tag(1)
  @DisplayMetadata(label = "JDBC URL (Ex: jdbc:mysql://localhost:3306/database?useUnicode=true&characterEncoding=utf8&autoReconnect=true)")
  public String jdbcURL;

  @Tag(2)
  @DisplayMetadata(label = "Username")
  public String username;

  @Tag(3)
  @Secret
  @DisplayMetadata(label = "Password")
  public String password;

  @Tag(4)
  @DisplayMetadata(label = "Record fetch size")
  @NotMetadataImpacting
  public int fetchSize = 2000;

  @VisibleForTesting
  public String toJdbcConnectionString() {
    checkNotNull(this.jdbcURL, "JDBC URL is required");
    return jdbcURL;
  }

  @Override
  @VisibleForTesting
  public Config toPluginConfig(SabotContext context) {
    logger.info("Connecting to Tidb");
    return JdbcStoragePlugin.Config.newBuilder()
        .withDialect(getDialect())
        .withFetchSize(fetchSize)
        .withDatasourceFactory(this::newDataSource)
        .clearHiddenSchemas()
        .addHiddenSchema("SYSTEM")
        .build();
  }

  private CloseableDataSource newDataSource() {
    return DataSources.newGenericConnectionPoolDataSource(DRIVER,
        toJdbcConnectionString(), username, password, null,
        DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE);
  }

  @Override
  public ArpDialect getDialect() {
    return ARP_DIALECT;
  }

  @VisibleForTesting
  public static ArpDialect getDialectSingleton() {
    return ARP_DIALECT;
  }
}