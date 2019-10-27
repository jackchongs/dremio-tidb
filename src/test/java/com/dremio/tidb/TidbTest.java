package com.dremio.tidb;


import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TidbTest {

  private static Logger log = Logger.getLogger(TidbTest.class);


  /* tidb connection settings */

  private static String authToken = "_dremio";

  private static final String tidbJdbcURL = System.getenv("TIDB_JDBC_URL");
  private static final String tidbUser = System.getenv("TIDB_USER");
  private static final String tidbPassword = System.getenv("TIDB_PASSWORD");

  @BeforeClass
  public static void setup() throws IOException, SQLException {

    log.info("Dremio: Get authentication token");
    CloseableHttpClient client = HttpClients.createDefault();
    HttpPost httpPost = new HttpPost("http://localhost:9047/apiv2/login");

    String json = "{\"userName\": \"admin\",\"password\": \"dremio\"}";

    httpPost.setEntity(new StringEntity(json));
    httpPost.setHeader("Content-type", "application/json");

    CloseableHttpResponse response = client.execute(httpPost);

    authToken = authToken + new ObjectMapper()
        .readTree(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8)).
            findValue("token").asText();

    client.close();
    log.info("authToken: " + authToken);
    // Create test table and insert sample data

    log.info("tidb: Create test table");
    Properties properties = new Properties();
    properties.put("user", tidbUser);
    properties.put("password", tidbPassword);

    Statement statement = DriverManager.getConnection(tidbJdbcURL, properties)
        .createStatement();
    log.info("statement: " + statement);
    String[] sqls = FileUtils
        .readFileToString(new File(new File("src/test/resources/DDL_tidb.sql").getPath()),
            StandardCharsets.UTF_8).split(";");
 
    // tidb doesn't support executing multiple SQLs in a single call
    statement.executeUpdate(sqls[0]);
    statement.executeUpdate(sqls[1]);

    log.info("Dremio: Create tidb datasource");
    CloseableHttpClient createSourceClient = HttpClients.createDefault();
    HttpPut httpPut = new HttpPut("http://localhost:9047/apiv2/source/tidb");

    String jsonPayload = String.format("{\n"
        + "    \"name\": \"tidb\",\n"
        + "    \"config\": {\n"
        + "        \"jdbcURL\": \"%s\",\n"
        + "        \"username\": \"%s\",\n"
        + "        \"password\": \"%s\",\n"
        + "        \"fetchSize\": 9047\n"
        + "    },\n"
        + "    \"accelerationRefreshPeriod\": 3600000,\n"
        + "    \"accelerationGracePeriod\": 10800000,\n"
        + "    \"metadataPolicy\": {\n"
        + "        \"deleteUnavailableDatasets\": true,\n"
        + "        \"namesRefreshMillis\": 3600000,\n"
        + "        \"datasetDefinitionRefreshAfterMillis\": 3600000,\n"
        + "        \"datasetDefinitionExpireAfterMillis\": 10800000,\n"
        + "        \"authTTLMillis\": 86400000,\n"
        + "        \"updateMode\": \"PREFETCH_QUERIED\"\n"
        + "    },\n"
        + "    \"accessControlList\": {\n"
        + "        \"userControls\": [],\n"
        + "        \"groupControls\": []\n"
        + "    },\n"
        + "    \"type\": \"tidb\"\n"
        + "}", tidbJdbcURL, tidbUser, tidbPassword);

    httpPut.setEntity(new StringEntity(jsonPayload));
    httpPut.setHeader("Content-type", "application/json");
    httpPut.setHeader("Authorization", authToken);

    assertEquals(200, createSourceClient.execute(httpPut).getStatusLine().getStatusCode());

    client.close();

  }


  @Test
  public void queryTest() throws IOException, SQLException {
    log.info("Dremio: SELECT * FROM all_types");

    TimeZone.setDefault(TimeZone.getTimeZone("Etc/UTC"));

    // Get resultset from Dremio

    Connection dremioConnection = DriverManager
        .getConnection("jdbc:dremio:direct=localhost:9047;user=admin;password=dremio");
    log.info(dremioConnection);
    Statement dremioStatement = dremioConnection.createStatement();

    ResultSet dremioRs = dremioStatement
        .executeQuery("select * from tidb.tpch.all_types");
    dremioRs.next();

    // Get resultset from tidb
    Properties properties = new Properties();
    properties.put("user", tidbUser);
    properties.put("password", tidbPassword);
    Connection tidbConnection = DriverManager
        .getConnection(tidbJdbcURL, properties);

    Statement tidbStatement = tidbConnection.createStatement();

    ResultSet tidbRs = tidbStatement
        .executeQuery("SELECT * FROM tpch.all_types");
    tidbRs.next();


    // Compare

    assertEquals(dremioRs.getBigDecimal("A"), tidbRs.getBigDecimal("A"));
    assertEquals(dremioRs.getInt("B"), tidbRs.getInt("B"));
    assertEquals(dremioRs.getInt("C"), tidbRs.getInt("C"));
    assertEquals(dremioRs.getInt("D"), tidbRs.getInt("D"));
    assertEquals(dremioRs.getInt("E"), tidbRs.getInt("E"));
    assertEquals(dremioRs.getInt("F"), tidbRs.getInt("F"));

    assertEquals(dremioRs.getDouble("G"), tidbRs.getDouble("G"),0.1);
    assertEquals(dremioRs.getDouble("H"), tidbRs.getDouble("H"),0.1);
    assertEquals(dremioRs.getDouble("I"), tidbRs.getDouble("I"),0.1);
    assertEquals(dremioRs.getDouble("J"), tidbRs.getDouble("J"),0.1);
    assertEquals(dremioRs.getDouble("K"), tidbRs.getDouble("K"),0.1);
    assertEquals(dremioRs.getDouble("L"), tidbRs.getDouble("L"),0.1);

    assertEquals(dremioRs.getString("M"), tidbRs.getString("M"));
    assertEquals(dremioRs.getString("N"), tidbRs.getString("N"));
    assertEquals(dremioRs.getString("O"), tidbRs.getString("O"));
    assertEquals(dremioRs.getString("P"), tidbRs.getString("P"));

    assertEquals(new String(dremioRs.getBytes("Q")), new String(tidbRs.getBytes("Q")));
    assertEquals(new String(dremioRs.getBytes("R")), new String(tidbRs.getBytes("R")));

    assertEquals(dremioRs.getTimestamp("S"), tidbRs.getTimestamp("S"));
    assertEquals(dremioRs.getTime("T"), tidbRs.getTime("T"));
    assertEquals(dremioRs.getTimestamp("U"), tidbRs.getTimestamp("U"));


    dremioStatement.close();
    tidbStatement.close();

  }

  @AfterClass
  public static void cleanUp() throws IOException, SQLException, InterruptedException {

    log.info("Dremio: Removing tidb data source in 5 seconds");

    CloseableHttpClient client = HttpClients.createDefault();
    HttpDelete httpDelete = new HttpDelete(
        "http://localhost:9047/apiv2/source/tidb?version=1");

    httpDelete.setHeader("Content-type", "application/json");
    httpDelete.setHeader("Authorization", authToken);

    client.execute(httpDelete);

    client.close();

    log.info("tidb: Remove test table");
    Properties properties = new Properties();
    properties.put("user", tidbUser);
    properties.put("password", tidbPassword);

    Statement statement = DriverManager.getConnection(tidbJdbcURL, properties)
        .createStatement();
    statement.executeUpdate("DROP TABLE \"tidb\".\"tpch\".\"all_types\"");
  }
}
