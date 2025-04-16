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

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

/**
 * Handles creation and interaction with the database.
 * Provides methods for table creation, log data insertion, and index setup.
 */
public class ContainerDatanodeDatabase {

  private static Map<String, String> queries;
  private static final int DEFAULT_LIMIT = 100;

  static {
    loadProperties();
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerDatanodeDatabase.class);

  private static void loadProperties() {
    Properties props = new Properties();
    try (InputStream inputStream = ContainerDatanodeDatabase.class.getClassLoader()
        .getResourceAsStream(DBConsts.PROPS_FILE)) {

      if (inputStream != null) {
        props.load(inputStream);
        queries = props.entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey().toString(),
                e -> e.getValue().toString()
            ));
      } else {
        throw new FileNotFoundException("Property file '" + DBConsts.PROPS_FILE + "' not found.");
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  private static Connection getConnection() throws Exception {
    Class.forName(DBConsts.DRIVER);

    SQLiteConfig config = new SQLiteConfig();

    config.setJournalMode(SQLiteConfig.JournalMode.OFF);
    config.setCacheSize(DBConsts.CACHE_SIZE);
    config.setLockingMode(SQLiteConfig.LockingMode.EXCLUSIVE);
    config.setSynchronous(SQLiteConfig.SynchronousMode.OFF);
    config.setTempStore(SQLiteConfig.TempStore.MEMORY);

    return DriverManager.getConnection(DBConsts.CONNECTION_PREFIX + DBConsts.DATABASE_NAME, config.toProperties());
  }

  public void createDatanodeContainerLogTable() throws SQLException {
    String createTableSQL = queries.get("CREATE_DATANODE_CONTAINER_LOG_TABLE");
    try (Connection connection = getConnection();
         Statement dropStmt = connection.createStatement();
         Statement createStmt = connection.createStatement()) {
      dropTable(DBConsts.DATANODE_CONTAINER_LOG_TABLE_NAME, dropStmt);
      createStmt.execute(createTableSQL);
      createDatanodeContainerIndex(createStmt);
    } catch (SQLException e) {
      LOG.error("Error while creating the table: {}", e.getMessage());
      throw e;
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new RuntimeException(e);
    }
  }

  private void createContainerLogTable() throws SQLException {
    String createTableSQL = queries.get("CREATE_CONTAINER_LOG_TABLE");
    try (Connection connection = getConnection();
         Statement dropStmt = connection.createStatement();
         Statement createStmt = connection.createStatement()) {
      dropTable(DBConsts.CONTAINER_LOG_TABLE_NAME, dropStmt);
      createStmt.execute(createTableSQL);
    } catch (SQLException e) {
      LOG.error("Error while creating the table: {}", e.getMessage());
      throw e;
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * Inserts a list of container log entries into the DatanodeContainerLogTable.
   *
   * @param transitionList List of container log entries to insert into the table.
   */
  
  public synchronized void insertContainerDatanodeData(List<DatanodeContainerInfo> transitionList) throws SQLException {

    String insertSQL = queries.get("INSERT_DATANODE_CONTAINER_LOG");

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
      LOG.error("Failed to insert container log for container {} on datanode {}", containerId, datanodeId, e);
      throw e;
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new RuntimeException(e);
    }
  }

  private void createDatanodeContainerIndex(Statement stmt) throws SQLException {
    String createIndexSQL = queries.get("CREATE_DATANODE_CONTAINER_INDEX");
    stmt.execute(createIndexSQL);
  }

  /**
   * Extracts the latest container log data from the DatanodeContainerLogTable
   * and inserts it into ContainerLogTable.
   */

  public void insertLatestContainerLogData() throws SQLException {
    createContainerLogTable();
    String selectSQL = queries.get("SELECT_LATEST_CONTAINER_LOG");
    String insertSQL = queries.get("INSERT_CONTAINER_LOG");

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
          LOG.error("Failed to insert container log entry for container {} on datanode {} ",
              containerId, datanodeId, e);
          throw e;
        }
      }

      if (count != 0) {
        insertStmt.executeBatch();
      }
    } catch (SQLException e) {
      LOG.error("Failed to insert container log entry: {}", e.getMessage());
      throw e;
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new RuntimeException(e);
    }
  }

  private void dropTable(String tableName, Statement stmt) throws SQLException {
    String dropTableSQL = queries.get("DROP_TABLE").replace("{table_name}", tableName);
    stmt.executeUpdate(dropTableSQL);
  }

  private void createContainerLogIndex(Statement stmt) throws SQLException {
    String createIndexSQL = queries.get("CREATE_INDEX_LATEST_STATE");
    stmt.execute(createIndexSQL);
  }

  /**
   * Lists containers filtered by the specified state and writes their details to a file or console.
   * <p>
   * The output includes timestamp, datanode ID, container ID, BCSID, error message, and index value,
   * written in a human-readable table format to a file or console.
   * - If no file path is provided and no limit is specified, results are printed
   *   to the console with a default limit of 100 rows.
   * - If no file path is provided but a limit is specified, results are printed
   *   to the console with the specified limit.
   * - If a file path is provided but no limit is specified, all matching rows are written
   *   to the specified file.
   * - If both a file path and a limit are provided, only the specified number of rows are written
   *   to the file.
   *
   * @param state the container state to filter by (e.g., "OPEN", "CLOSED", "QUASI_CLOSED")
   * @param path the file path to write the output to. if {@code null}, output is printed to the console.
   * @param limit the maximum number of rows to return.
   *
   */

  public void listContainersByState(String state, String path, Integer limit) throws SQLException {
    int count = 0;

    boolean writeToFile = path != null;
    boolean limitProvided = limit != null;

    String baseQuery = queries.get("SELECT_LATEST_CONTAINER_LOGS_BY_STATE");
    String finalQuery = (!writeToFile && !limitProvided) ? baseQuery + " LIMIT " + DEFAULT_LIMIT
        : (limitProvided ? baseQuery + " LIMIT ?" : baseQuery);

    Path outputPath = null;
    if (writeToFile) {
      outputPath = Paths.get(path);
      if (Files.exists(outputPath)) {
        System.out.println("Warning: Output file already exists and will be overwritten: "
            + outputPath.toAbsolutePath());
      }
    }

    try (Connection connection = getConnection();
         Statement stmt = connection.createStatement()) {

      createContainerLogIndex(stmt);

      try (PreparedStatement pstmt = connection.prepareStatement(finalQuery)) {
        pstmt.setString(1, state);
        if (limitProvided) {
          pstmt.setInt(2, limit);
        }

        try (ResultSet rs = pstmt.executeQuery();
             BufferedWriter writer = writeToFile
                 ? Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)
                 : new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8))) {

          writer.write(String.format("%-25s | %-35s | %-15s | %-15s | %-40s | %-12s%n",
              "Timestamp", "Datanode ID", "Container ID", "BCSID", "Message", "Index Value"));
          writer.write("-------------------------------------------------------------------------------------" +
              "---------------------------------------------------------------------------------------\n");

          while (rs.next()) {
            String timestamp = rs.getString("timestamp");
            String datanodeId = rs.getString("datanode_id");
            long containerId = rs.getLong("container_id");
            long latestBcsid = rs.getLong("latest_bcsid");
            String errorMessage = rs.getString("error_message");
            int indexValue = rs.getInt("index_value");
            count++;

            writer.write(String.format("%-25s | %-35s | %-15d | %-15d | %-40s | %-12d%n",
                timestamp, datanodeId, containerId, latestBcsid, errorMessage, indexValue));
          }
          
          if (count == 0) {
            writer.write("No containers found for state: " + state + "\n");
          } else {
            writer.write("total: " + count + "\n");
            writer.flush();
            if (writeToFile) {
              System.out.println("Results written to file: " + outputPath.toAbsolutePath());
            }
          }
        }
      }
    } catch (SQLException e) {
      LOG.error("Error while retrieving containers with state: {}", state, e);
      throw e;
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new RuntimeException(e);
    }
  }
}

