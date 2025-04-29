/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.containerlog.parser;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.sqlite.SQLiteConfig;

/**
 * Handles creation and interaction with the database.
 * Provides methods for table creation, log data insertion, and index setup.
 */
public class ContainerDatanodeDatabase {
  
  private static String databasePath;
  
  public static void setDatabasePath(String dbPath) {
    if (databasePath == null) {
      databasePath = dbPath;
    }
  }

  private static Connection getConnection() throws Exception {
    if (databasePath == null) {
      throw new IllegalStateException("Database path not set");
    }
    
    Class.forName(DBConsts.DRIVER);

    SQLiteConfig config = new SQLiteConfig();

    config.setJournalMode(SQLiteConfig.JournalMode.OFF);
    config.setCacheSize(DBConsts.CACHE_SIZE);
    config.setLockingMode(SQLiteConfig.LockingMode.EXCLUSIVE);
    config.setSynchronous(SQLiteConfig.SynchronousMode.OFF);
    config.setTempStore(SQLiteConfig.TempStore.MEMORY);

    return DriverManager.getConnection(DBConsts.CONNECTION_PREFIX + databasePath, config.toProperties());
  }

  public void createDatanodeContainerLogTable() throws SQLException {
    String createTableSQL = DBConsts.CREATE_DATANODE_CONTAINER_LOG_TABLE;
    try (Connection connection = getConnection();
         Statement dropStmt = connection.createStatement();
         Statement createStmt = connection.createStatement()) {
      dropTable(DBConsts.DATANODE_CONTAINER_LOG_TABLE_NAME, dropStmt);
      createStmt.execute(createTableSQL);
      createDatanodeContainerIndex(createStmt);
    } catch (SQLException e) {
      System.err.println("Error while creating the table: " + e.getMessage());
      throw e;
    } catch (Exception e) {
      System.err.println("Unexpected error: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  private void createContainerLogTable() throws SQLException {
    String createTableSQL = DBConsts.CREATE_CONTAINER_LOG_TABLE;
    try (Connection connection = getConnection();
         Statement dropStmt = connection.createStatement();
         Statement createStmt = connection.createStatement()) {
      dropTable(DBConsts.CONTAINER_LOG_TABLE_NAME, dropStmt);
      createStmt.execute(createTableSQL);
    } catch (SQLException e) {
      System.err.println("Error while creating the table: " + e.getMessage());
      throw e;
    } catch (Exception e) {
      System.err.println("Unexpected error: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * Inserts a list of container log entries into the DatanodeContainerLogTable.
   *
   * @param transitionList List of container log entries to insert into the table.
   */
  
  public synchronized void insertContainerDatanodeData(List<DatanodeContainerInfo> transitionList) throws SQLException {

    String insertSQL = DBConsts.INSERT_DATANODE_CONTAINER_LOG;

    long containerId = 0;
    String datanodeId = null;
    
    try (Connection connection = getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {

      int count = 0;

      for (DatanodeContainerInfo info : transitionList) {
        datanodeId = info.getDatanodeId();
        containerId = info.getContainerId();

        preparedStatement.setString(1, datanodeId);
        preparedStatement.setLong(2, containerId);
        preparedStatement.setString(3, info.getTimestamp());
        preparedStatement.setString(4, info.getState());
        preparedStatement.setLong(5, info.getBcsid());
        preparedStatement.setString(6, info.getErrorMessage());
        preparedStatement.setString(7, info.getLogLevel());
        preparedStatement.setInt(8, info.getIndexValue());
        preparedStatement.addBatch();

        count++;

        if (count % DBConsts.BATCH_SIZE == 0) {
          preparedStatement.executeBatch();
          count = 0;
        }
      }

      if (count != 0) {
        preparedStatement.executeBatch();
      }
    } catch (SQLException e) {
      System.err.println("Failed to insert container log for container " + containerId + " on datanode " + datanodeId);
      throw e;
    } catch (Exception e) {
      System.err.println("Unexpected error: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  private void createDatanodeContainerIndex(Statement stmt) throws SQLException {
    String createIndexSQL = DBConsts.CREATE_DATANODE_CONTAINER_INDEX;
    stmt.execute(createIndexSQL);
  }

  /**
   * Extracts the latest container log data from the DatanodeContainerLogTable
   * and inserts it into ContainerLogTable.
   */

  public void insertLatestContainerLogData() throws SQLException {
    createContainerLogTable();
    String selectSQL = DBConsts.SELECT_LATEST_CONTAINER_LOG;
    String insertSQL = DBConsts.INSERT_CONTAINER_LOG;

    try (Connection connection = getConnection();
         PreparedStatement selectStmt = connection.prepareStatement(selectSQL);
         ResultSet resultSet = selectStmt.executeQuery();
         PreparedStatement insertStmt = connection.prepareStatement(insertSQL)) {
      
      int count = 0;
      
      while (resultSet.next()) {
        String datanodeId = resultSet.getString("datanode_id");
        long containerId = resultSet.getLong("container_id");
        String containerState = resultSet.getString("container_state");
        long bcsid = resultSet.getLong("bcsid");
        try {
          insertStmt.setString(1, datanodeId);
          insertStmt.setLong(2, containerId);
          insertStmt.setString(3, containerState);
          insertStmt.setLong(4, bcsid);
          insertStmt.addBatch();

          count++;

          if (count % DBConsts.BATCH_SIZE == 0) {
            insertStmt.executeBatch();
            count = 0;
          }
        } catch (SQLException e) {
          System.err.println("Failed to insert container log entry for container " + containerId + " on datanode "
              + datanodeId);
          throw e;
        }
      }

      if (count != 0) {
        insertStmt.executeBatch();
      }
    } catch (SQLException e) {
      System.err.println("Failed to insert container log entry: " + e.getMessage());
      throw e;
    } catch (Exception e) {
      System.err.println("Unexpected error: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  private void dropTable(String tableName, Statement stmt) throws SQLException {
    String dropTableSQL = DBConsts.DROP_TABLE.replace("{table_name}", tableName);
    stmt.executeUpdate(dropTableSQL);
  }

  private void createContainerLogIndex(Statement stmt) throws SQLException {
    String createIndexSQL = DBConsts.CREATE_INDEX_LATEST_STATE;
    stmt.execute(createIndexSQL);
  }

  /**
   * Lists containers filtered by the specified state and writes their details to stdout 
   * unless redirected to a file explicitly.
   * The output includes timestamp, datanode ID, container ID, BCSID, error message, and index value,
   * written in a human-readable table format to a file or console.
   * Behavior based on the {@code limit} parameter:
   * If {@code limit} is provided, only up to the specified number of rows are printed.
   * If the number of matching containers exceeds the {@code limit},
   * a note is printed indicating more containers exist.
   *
   * @param state the container state to filter by (e.g., "OPEN", "CLOSED", "QUASI_CLOSED")
   * @param limit the maximum number of rows to display; use {@link Integer#MAX_VALUE} to fetch all rows
   */

  public void listContainersByState(String state, Integer limit) throws SQLException {
    int count = 0;

    boolean limitProvided = limit != Integer.MAX_VALUE;

    String baseQuery = DBConsts.SELECT_LATEST_CONTAINER_LOGS_BY_STATE;
    String finalQuery = limitProvided ? baseQuery + " LIMIT ?" : baseQuery;

    try (Connection connection = getConnection();
         Statement stmt = connection.createStatement()) {

      createContainerLogIndex(stmt);

      try (PreparedStatement pstmt = connection.prepareStatement(finalQuery)) {
        pstmt.setString(1, state);
        if (limitProvided) {
          pstmt.setInt(2, limit + 1);
        }

        try (ResultSet rs = pstmt.executeQuery();
             PrintWriter writer = new PrintWriter(new OutputStreamWriter(System.out,
                 StandardCharsets.UTF_8), true)) {

          writer.printf("%-25s | %-35s | %-15s | %-15s | %-40s | %-12s%n",
              "Timestamp", "Datanode ID", "Container ID", "BCSID", "Message", "Index Value");
          writer.println("-------------------------------------------------------------------------------------" +
              "---------------------------------------------------------------------------------------");

          while (rs.next()) {
            if (limitProvided && count >= limit) {
              writer.println("Note: There might be more containers. Use -all option to list all entries");
              break;
            }
            String timestamp = rs.getString("timestamp");
            String datanodeId = rs.getString("datanode_id");
            long containerId = rs.getLong("container_id");
            long latestBcsid = rs.getLong("latest_bcsid");
            String errorMessage = rs.getString("error_message");
            int indexValue = rs.getInt("index_value");
            count++;

            writer.printf("%-25s | %-35s | %-15d | %-15d | %-40s | %-12d%n",
                timestamp, datanodeId, containerId, latestBcsid, errorMessage, indexValue);
          }
          
          if (count == 0) {
            writer.printf("No containers found for state: %s%n", state);
          } else {
            writer.printf("Number of containers listed: %d%n", count);
          }
        }
      }
    } catch (SQLException e) {
      throw new SQLException("Error while retrieving containers with state " + state);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

