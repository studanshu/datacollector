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
package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.destination.jdbc.Errors;
import com.streamsets.pipeline.stage.destination.jdbc.JdbcFieldMappingConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class JdbcGenericRecordWriter extends JdbcBaseRecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcGenericRecordWriter.class);

  /**
   * Class constructor
   * @param connectionString database connection string
   * @param dataSource a JDBC {@link javax.sql.DataSource} to get a connection from
   * @param tableName the name of the table to write to
   * @param rollbackOnError whether to attempt rollback of failed queries
   * @param customMappings any custom mappings the user provided
   * @throws StageException
   */
  public JdbcGenericRecordWriter(
      String connectionString,
      DataSource dataSource,
      String tableName,
      boolean rollbackOnError,
      List<JdbcFieldMappingConfig> customMappings
  ) throws StageException {
    super(connectionString, dataSource, tableName, rollbackOnError, customMappings);
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  public List<OnRecordErrorException> writeBatch(Collection<Record> batch) throws StageException {
    List<OnRecordErrorException> errorRecords = new LinkedList<>();
    Connection connection = null;
    try {
      connection = getDataSource().getConnection();

      PreparedStatementMap statementsForBatch = new PreparedStatementMap(connection, getTableName());

      for (Record record : batch) {
        Map<String, String> parameters = getColumnsToParameters();
        SortedMap<String, String> columnsToParameters = new TreeMap<>();
        for (Map.Entry<String, String> entry : getColumnsToFields().entrySet()) {
          String columnName = entry.getKey();
          String fieldPath = entry.getValue();

          if (record.has(fieldPath)) {
            columnsToParameters.put(columnName, parameters.get(columnName));
          }
        }
        PreparedStatement statement = statementsForBatch.getInsertFor(columnsToParameters);

        if (setParameters(columnsToParameters, record, connection, statement, errorRecords)) {
          statement.addBatch();
        }
      }

      for (PreparedStatement statement : statementsForBatch.getStatements()) {
        try {
          statement.executeBatch();
        } catch (SQLException e) {
          if (getRollbackOnError()) {
            connection.rollback();
          }
          handleBatchUpdateException(batch, e, errorRecords);
        }
      }
      connection.commit();
    } catch (SQLException e) {
      handleSqlException(e);
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          handleSqlException(e);
        }
      }
    }
    return errorRecords;
  }

  @SuppressWarnings("unchecked")
  private boolean setParameters(
      Map<String, String> columnsToParameters,
      Record record,
      Connection connection,
      PreparedStatement statement,
      List<OnRecordErrorException> errorRecords
  ) {
    boolean isOk = true;
    int paramIdx = 1;
    for (String column : columnsToParameters.keySet()) {
      Field field = record.get(getColumnsToFields().get(column));
      Field.Type fieldType = field.getType();
      Object value = field.getValue();

      try {
        switch (fieldType) {
          case LIST:
            List<Object> unpackedList = unpackList((List<Field>) value);
            Array array = connection.createArrayOf(getSQLTypeName(fieldType), unpackedList.toArray());
            statement.setArray(paramIdx, array);
            break;
          case DATE:
          case TIME:
          case DATETIME:
            // Java Date types are not accepted by JDBC drivers, so we need to convert to java.sql.Timestamp
            statement.setTimestamp(paramIdx, new java.sql.Timestamp(field.getValueAsDatetime().getTime()));
            break;
          default:
            statement.setObject(paramIdx, value, getColumnType(column));
            break;
        }
      } catch (SQLException e) {
        LOG.error(Errors.JDBCDEST_23.getMessage(), column, fieldType.toString(), e);
        errorRecords.add(new OnRecordErrorException(record, Errors.JDBCDEST_23, column, fieldType.toString()));
        isOk = false;
      } catch (OnRecordErrorException e) {
        LOG.error(e.toString(), e);
        errorRecords.add(e);
        isOk = false;
      }
      ++paramIdx;
    }
    return isOk;
  }

  /**
   * This is an error that is not due to bad input record and should throw a StageException
   * once we format the error.
   * @param e SQLException
   * @throws StageException
   */
  private void handleSqlException(SQLException e) throws StageException {
    String formattedError = JdbcUtil.formatSqlException(e);
    LOG.error(formattedError);
    LOG.debug(formattedError, e);
    throw new StageException(Errors.JDBCDEST_14, formattedError);
  }

  /**
   * <p>
   *   Some databases drivers allow us to figure out which record in a particular batch failed.
   * </p>
   * <p>
   *   In the case that we have a list of update counts, we can mark just the record as erroneous.
   *   Otherwise we must send the entire batch to error.
   * </p>
   * @param failedRecords List of Failed Records
   * @param e BatchUpdateException
   * @param errorRecords List of error records for this batch
   */
  private void handleBatchUpdateException(
      Collection<Record> failedRecords,
      SQLException e,
      List<OnRecordErrorException> errorRecords
  ) throws StageException {
    if (JdbcUtil.isDataError(getConnectionString(), e)) {
      String formattedError = JdbcUtil.formatSqlException(e);
      LOG.error(formattedError);
      LOG.debug(formattedError, e);

      if (!getRollbackOnError() && e instanceof BatchUpdateException &&
          ((BatchUpdateException) e).getUpdateCounts().length > 0) {
        BatchUpdateException bue = (BatchUpdateException) e;

        int i = 0;
        for (Record record : failedRecords) {
          if (i >= bue.getUpdateCounts().length ||
              bue.getUpdateCounts()[i] == PreparedStatement.EXECUTE_FAILED) {
            errorRecords.add(new OnRecordErrorException(record, Errors.JDBCDEST_14, formattedError));
          }
          i++;
        }
      } else {
        for (Record record : failedRecords) {
          errorRecords.add(new OnRecordErrorException(record, Errors.JDBCDEST_14, formattedError));
        }
      }
    } else {
      handleSqlException(e);
    }
  }
}
