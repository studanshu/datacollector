/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.jdbc;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class TestJdbcSource {
  private static final Logger LOG = LoggerFactory.getLogger(TestJdbcSource.class);
  private static final int BATCH_SIZE = 1000;
  private static final int CLOB_SIZE = 1000;

  private final String username = "sa";
  private final String password = "sa";
  private final String database = "test";

  private final String h2ConnectionString = "jdbc:h2:mem:" + database;
  private final String query = "SELECT * FROM TEST.TEST_TABLE WHERE P_ID > ${offset} ORDER BY P_ID ASC LIMIT 10;";
  private final String initialOffset = "0";
  private final long queryInterval = 0L;

  private Connection connection = null;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws SQLException {
    // Create a table in H2 and put some data in it for querying.
    connection = DriverManager.getConnection(h2ConnectionString, username, password);
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.addBatch("CREATE SCHEMA IF NOT EXISTS TEST;");
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_TABLE " +
              "(p_id INT NOT NULL, first_name VARCHAR(255), last_name VARCHAR(255), UNIQUE(p_id));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_ARRAY " +
              "(p_id INT NOT NULL, non_scalar ARRAY, UNIQUE(p_id));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_LOB " +
              "(p_id INT NOT NULL, clob_col CLOB, blob_col BLOB, UNIQUE(p_id));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_JDBC_NS_HEADERS " +
              "(p_id INT NOT NULL, dec DECIMAL(2, 1));"
      );
      statement.addBatch(
          "CREATE TABLE IF NOT EXISTS TEST.TEST_NULL " +
              "(p_id INT NOT NULL, name VARCHAR(255), number int);"
      );
      statement.addBatch(
        "CREATE TABLE IF NOT EXISTS TEST.TEST_TIMES " +
          "(p_id INT NOT NULL, d DATE, t TIME, ts TIMESTAMP);"
      );
      // Add some data
      statement.addBatch("INSERT INTO TEST.TEST_TABLE VALUES (1, 'Adam', 'Kunicki')");
      statement.addBatch("INSERT INTO TEST.TEST_TABLE VALUES (2, 'Jon', 'Natkins')");
      statement.addBatch("INSERT INTO TEST.TEST_TABLE VALUES (3, 'Jon', 'Daulton')");
      statement.addBatch("INSERT INTO TEST.TEST_TABLE VALUES (4, 'Girish', 'Pancha')");
      statement.addBatch("INSERT INTO TEST.TEST_ARRAY VALUES (1, (1,2,3))");
      statement.addBatch("INSERT INTO TEST.TEST_LOB VALUES  (1, 'short string for clob', RAWTOHEX('blob_val'))");
      statement.addBatch("INSERT INTO TEST.TEST_LOB VALUES  (2, 'long string for clob" +
          RandomStringUtils.randomAlphanumeric(CLOB_SIZE) + "', RAWTOHEX('blob_val" +
          RandomStringUtils.randomAlphanumeric(CLOB_SIZE) + "'))");
      statement.addBatch("INSERT INTO TEST.TEST_JDBC_NS_HEADERS VALUES  (1, 1.5)");
      statement.addBatch("INSERT INTO TEST.TEST_NULL VALUES  (1, NULL, NULL)");
      statement.addBatch("INSERT INTO TEST.TEST_TIMES VALUES  (1, '1993-09-01', '15:09:02', '1960-01-01 23:03:20')");
      statement.executeBatch();
    }
  }

  @After
  public void tearDown() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      // Setup table
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_TABLE;");
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_ARRAY;");
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_LOB;");
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_JDBC_NS_HEADERS;");
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_NULL");
      statement.execute("DROP TABLE IF EXISTS TEST.TEST_TIMES");
    }

    // Last open connection terminates H2
    connection.close();
  }

  private HikariPoolConfigBean createConfigBean(String connectionString, String username, String password) {
    HikariPoolConfigBean bean = new HikariPoolConfigBean();
    bean.connectionString = connectionString;
    bean.username = username;
    bean.password = password;

    return bean;
  }

  @Test
  public void testIncrementalMode() throws Exception {
    JdbcSource origin = new JdbcSource(
        true,
        query,
        initialOffset,
        "P_ID",
        queryInterval,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        "",
        createConfigBean(h2ConnectionString, username, password)
        );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 2);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(2, parsedRecords.size());

      assertEquals("2", output.getNewOffset());

      // Check that the remaining rows in the initial cursor are read.
      output = runner.runProduce(output.getNewOffset(), 100);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(2, parsedRecords.size());


      // Check that new rows are loaded.
      runInsertNewRows();
      output = runner.runProduce(output.getNewOffset(), 100);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(2, parsedRecords.size());

      assertEquals("10", output.getNewOffset());

      // Check that older rows are not loaded.
      runInsertOldRows();
      output = runner.runProduce(output.getNewOffset(), 100);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(0, parsedRecords.size());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testNonIncrementalMode() throws Exception {
    JdbcSource origin = new JdbcSource(
        false,
        query,
        initialOffset,
        "P_ID",
        queryInterval,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        "",
        createConfigBean(h2ConnectionString, username, password)
        );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 2);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(2, parsedRecords.size());

      assertEquals(initialOffset, output.getNewOffset());

      // Check that the remaining rows in the initial cursor are read.
      output = runner.runProduce(output.getNewOffset(), 100);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(2, parsedRecords.size());


      // Check that new rows are loaded.
      runInsertNewRows();
      output = runner.runProduce(output.getNewOffset(), 100);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(6, parsedRecords.size());

      assertEquals(initialOffset, output.getNewOffset());

      // Check that older rows are loaded.
      runInsertOldRows();
      output = runner.runProduce(output.getNewOffset(), 100);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(8, parsedRecords.size());
    } finally {
      runner.runDestroy();
    }
  }

  private void runInsertNewRows() throws SQLException {
    try (Connection connection = DriverManager.getConnection(h2ConnectionString, username, password)) {
      try (Statement statement = connection.createStatement()) {
        // Add some data
        statement.addBatch("INSERT INTO TEST.TEST_TABLE VALUES (9, 'Arvind', 'Prabhakar')");
        statement.addBatch("INSERT INTO TEST.TEST_TABLE VALUES (10, 'Brock', 'Noland')");

        statement.executeBatch();
      }
    }
  }

  @Test
  public void testBadConnectionString() throws Exception {
    JdbcSource origin = new JdbcSource(
        true,
        query,
        initialOffset,
        "P_ID",
        queryInterval,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        "",
        createConfigBean("some bad connection string", username, password)
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testMissingWhereClause() throws Exception {
    String queryMissingWhere = "SELECT * FROM TEST.TEST_TABLE ORDER BY P_ID ASC LIMIT 10;";
    JdbcSource origin = new JdbcSource(
        true,
        queryMissingWhere,
        initialOffset,
        "P_ID",
        queryInterval,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        "",
        createConfigBean(h2ConnectionString, username, password)
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains("Query must include 'WHERE' clause."));
  }

  @Test
  public void testMissingOrderByClause() throws Exception {
    String queryMissingOrderBy = "SELECT * FROM TEST.TEST_TABLE WHERE P_ID > ${offset} LIMIT 10;";
    JdbcSource origin = new JdbcSource(
        true,
        queryMissingOrderBy,
        initialOffset,
        "P_ID",
        queryInterval,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        "",
        createConfigBean(h2ConnectionString, username, password)
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    for (Stage.ConfigIssue issue : issues) {
      LOG.info(issue.toString());
    }
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains("Query must include 'ORDER BY' clause."));
  }

  @Test
  public void testMissingWhereAndOrderByClause() throws Exception {
    String queryMissingWhereAndOrderBy = "SELECT * FROM TEST.TEST_TABLE;";
    JdbcSource origin = new JdbcSource(
        true,
        queryMissingWhereAndOrderBy,
        initialOffset,
        "P_ID",
        queryInterval,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        "",
        createConfigBean(h2ConnectionString, username, password)
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(2, issues.size());
  }

  @Test
  public void testInvalidQuery() throws Exception {
    String queryInvalid = "SELET * FORM TABLE WHERE P_ID > ${offset} ORDER BY P_ID LIMIT 10;";
    JdbcSource origin = new JdbcSource(
        true,
        queryInvalid,
        initialOffset,
        "P_ID",
        queryInterval,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        "",
        createConfigBean(h2ConnectionString, username, password)
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testMultiLineQuery() throws Exception {
    String queryInvalid = "SELECT * FROM TEST.TEST_TABLE WHERE\nP_ID > ${offset}\nORDER BY P_ID ASC LIMIT 10;";
    JdbcSource origin = new JdbcSource(
        true,
        queryInvalid,
        initialOffset,
        "P_ID",
        queryInterval,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        "",
        createConfigBean(h2ConnectionString, username, password)
      );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());
  }

  private void runInsertOldRows() throws SQLException {
    try (Connection connection = DriverManager.getConnection(h2ConnectionString, username, password)) {
      try (Statement statement = connection.createStatement()) {
        // Add some data
        statement.addBatch("INSERT INTO TEST.TEST_TABLE VALUES (5, 'Arvind', 'Prabhakar')");
        statement.addBatch("INSERT INTO TEST.TEST_TABLE VALUES (6, 'Brock', 'Noland')");

        statement.executeBatch();
      }
    }
  }

  @Test
  public void testCdcMode() throws Exception {
    JdbcSource origin = new JdbcSource(
        true,
        query,
        "1",
        "P_ID",
        queryInterval,
        "FIRST_NAME",
        1000,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        "",
        createConfigBean(h2ConnectionString, username, password)
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(2, parsedRecords.size());

      assertEquals("3", output.getNewOffset());

      // Check that the next 'transaction' of 1 row is read.
      output = runner.runProduce(output.getNewOffset(), 1000);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(1, parsedRecords.size());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testCdcSplitTransactionMode() throws Exception {
    JdbcSource origin = new JdbcSource(
        true,
        query,
        "1",
        "P_ID",
        queryInterval,
        "FIRST_NAME",
        1,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        "",
        createConfigBean(h2ConnectionString, username, password)
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(1, parsedRecords.size());
      assertEquals("2", output.getNewOffset());

      // Check that the next 'transaction' of 1 row is read.
      output = runner.runProduce(output.getNewOffset(), 1000);
      parsedRecords = output.getRecords().get("lane");

      assertEquals(1, parsedRecords.size());
      assertEquals("3", output.getNewOffset());

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testQualifiedOffsetColumnInQuery() throws Exception {
    final String query = "SELECT * FROM TEST.TEST_TABLE T WHERE T.P_ID > ${offset} ORDER BY T.P_ID ASC LIMIT 10;";

    JdbcSource origin = new JdbcSource(
        true,
        query,
        "1",
        "P_ID",
        queryInterval,
        "FIRST_NAME",
        1,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        "",
        createConfigBean(h2ConnectionString, username, password)
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());
  }

  @Test
  public void testDuplicateColumnLabels() throws Exception {
    final String query = "SELECT * FROM TEST.TEST_TABLE T, TEST.TEST_TABLE TB WHERE T.P_ID > ${offset} " +
        "ORDER BY T.P_ID ASC LIMIT 10;";

    JdbcSource origin = new JdbcSource(
        true,
        query,
        "1",
        "P_ID",
        queryInterval,
        "FIRST_NAME",
        1,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        "",
        createConfigBean(h2ConnectionString, username, password)
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(3, issues.size());
  }

  @Test
  public void testPrefixedOffsetColumn() throws Exception {
    final String query = "SELECT * FROM TEST.TEST_TABLE T WHERE T.P_ID > ${offset} ORDER BY T.P_ID ASC LIMIT 10;";

    JdbcSource origin = new JdbcSource(
        true,
        query,
        "1",
        "T.P_ID",
        queryInterval,
        "FIRST_NAME",
        1,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        "",
        createConfigBean(h2ConnectionString, username, password)
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
  }

  @Test
  public void testEmptyResultSet() throws Exception {
    Statement statement = connection.createStatement();
    statement.execute("TRUNCATE TABLE TEST.TEST_TABLE");

    JdbcSource origin = new JdbcSource(
        true,
        query,
        initialOffset,
        "P_ID",
        queryInterval,
        "FIRST_NAME",
        1000,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        "",
        createConfigBean(h2ConnectionString, username, password)
    );
    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(0, parsedRecords.size());

      assertEquals(initialOffset, output.getNewOffset());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testLobColumns() throws Exception {
    String queryClob = "SELECT * FROM TEST.TEST_LOB WHERE P_ID > ${offset} ORDER BY P_ID ASC LIMIT 10;";
    JdbcSource origin = new JdbcSource(
        true,
        queryClob,
        initialOffset,
        "P_ID",
        queryInterval,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        "",
        createConfigBean(h2ConnectionString, username, password)
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(2, parsedRecords.size());

      assertEquals("2", output.getNewOffset());

      // First record is shorter than CLOB_SIZE, so it must be as is.
      assertEquals("short string for clob", parsedRecords.get(0).get("/CLOB_COL").getValueAsString());
      assertEquals("blob_val", IOUtils.toString(parsedRecords.get(0).get("/BLOB_COL").getValueAsByteArray(), "UTF-16"));

      // Second record is longer than CLOB_SIZE, so it must be truncated.
      assertEquals(CLOB_SIZE, parsedRecords.get(1).get("/CLOB_COL").getValueAsString().length());
      assertEquals(CLOB_SIZE, parsedRecords.get(1).get("/BLOB_COL").getValueAsByteArray().length);
      assertTrue(parsedRecords.get(1).get("/CLOB_COL").getValueAsString().startsWith("long string for clob"));
      assertTrue(IOUtils.toString(parsedRecords.get(0).get("/BLOB_COL").getValueAsByteArray(), "UTF-16").startsWith("blob_val"));
    } finally {
      runner.runDestroy();
    }
  }
  @Test
  public void testJDBCNsHeaders() throws Exception{
    String queryDecimal = "SELECT * from TEST.TEST_JDBC_NS_HEADERS T WHERE T.P_ID > ${offset} ORDER BY T.P_ID ASC LIMIT 10;";
    JdbcSource origin = new JdbcSource(
        true,
        queryDecimal,
        initialOffset,
        "P_ID",
        queryInterval,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        true,
        "jdbc.",
        createConfigBean(h2ConnectionString, username, password)
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(1, parsedRecords.size());
      assertEquals("1", output.getNewOffset());

      Record parsedRecord = parsedRecords.get(0);
      assertEquals(new BigDecimal(1.5), parsedRecord.get("/DEC").getValueAsDecimal());
      assertEquals("1", parsedRecord.getHeader().getAttribute("jdbc.DEC.scale"));
      assertEquals("2", parsedRecord.getHeader().getAttribute("jdbc.DEC.precision"));
      assertEquals(String.valueOf(Types.DECIMAL), parsedRecord.getHeader().getAttribute("jdbc.DEC.jdbcType"));
      assertEquals("TEST_JDBC_NS_HEADERS", parsedRecord.getHeader().getAttribute("jdbc.tables"));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testImportingNull() throws Exception {
    String query = "SELECT * from TEST.TEST_NULL T WHERE T.P_ID > ${offset} ORDER BY T.P_ID ASC LIMIT 10;";
    JdbcSource origin = new JdbcSource(
        true,
        query,
        initialOffset,
        "P_ID",
        queryInterval,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        null,
        createConfigBean(h2ConnectionString, username, password)
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(1, parsedRecords.size());
      assertEquals("1", output.getNewOffset());

      Record parsedRecord = parsedRecords.get(0);
      assertTrue(parsedRecord.has("/NAME"));
      assertEquals(Field.Type.STRING, parsedRecord.get("/NAME").getType());
      assertNull(parsedRecord.get("/NAME").getValue());

      assertTrue(parsedRecord.has("/NUMBER"));
      assertEquals(Field.Type.INTEGER, parsedRecord.get("/NUMBER").getType());
      assertNull(parsedRecord.get("/NUMBER").getValue());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testTimeTypes() throws Exception {
    String query = "SELECT * from TEST.TEST_TIMES T WHERE T.P_ID > ${offset} ORDER BY T.P_ID ASC LIMIT 10;";
    JdbcSource origin = new JdbcSource(
        true,
        query,
        initialOffset,
        "P_ID",
        queryInterval,
        "",
        1000,
        JdbcRecordType.LIST_MAP,
        BATCH_SIZE,
        CLOB_SIZE,
        CLOB_SIZE,
        false,
        null,
        createConfigBean(h2ConnectionString, username, password)
    );

    SourceRunner runner = new SourceRunner.Builder(JdbcDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(1, parsedRecords.size());
      assertEquals("1", output.getNewOffset());

      Record parsedRecord = parsedRecords.get(0);
      assertTrue(parsedRecord.has("/D"));
      assertEquals(Field.Type.DATE, parsedRecord.get("/D").getType());

      assertTrue(parsedRecord.has("/T"));
      assertEquals(Field.Type.TIME, parsedRecord.get("/T").getType());

      assertTrue(parsedRecord.has("/TS"));
      assertEquals(Field.Type.DATETIME, parsedRecord.get("/TS").getType());
    } finally {
      runner.runDestroy();
    }
  }
}
