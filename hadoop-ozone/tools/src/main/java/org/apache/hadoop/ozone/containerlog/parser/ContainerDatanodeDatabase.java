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

import java.io.FileNotFoundException;
import java.io.InputStream;
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
import org.sqlite.SQLiteConfig;

/**
 * Datanode container Database.
 */

public class ContainerDatanodeDatabase {

  private static Map<String, String> queries;

  static {
    loadProperties();
  }

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
      e.printStackTrace();
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

  public void createDatanodeContainerLogTable() {
    String createTableSQL = queries.get("CREATE_DATANODE_CONTAINER_LOG_TABLE");
    try (Connection connection = getConnection();
         Statement stmt = connection.createStatement()) {
      stmt.execute(createTableSQL);
    } catch (SQLException e) {
      System.err.println("Error while creating the table: " + e.getMessage());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void createContainerLogTable() {
    String createTableSQL = queries.get("CREATE_CONTAINER_LOG_TABLE");
    try (Connection connection = getConnection();
         Statement stmt = connection.createStatement()) {
      stmt.execute(createTableSQL);
    } catch (SQLException e) {
      System.err.println("Error while creating the table: " + e.getMessage());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void insertContainerDatanodeData(String key, List<DatanodeContainerInfo> transitionList) {
    String[] parts = key.split("#");
    if (parts.length != 2) {
      System.err.println("Invalid key format: " + key);
      return;
    }

    long containerId = Long.parseLong(parts[0]);
    long datanodeId = Long.parseLong(parts[1]);

    String insertSQL = queries.get("INSERT_DATANODE_CONTAINER_LOG");

    try (Connection connection = getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {

      int count = 0;

      for (DatanodeContainerInfo info : transitionList) {
        preparedStatement.setLong(1, datanodeId);
        preparedStatement.setLong(2, containerId);
        preparedStatement.setString(3, info.getTimestamp());
        preparedStatement.setString(4, info.getState());
        preparedStatement.setLong(5, info.getBcsid());
        preparedStatement.setString(6, info.getErrorMessage());
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
      System.err.println("Error while inserting data: " + e.getMessage());
      for (DatanodeContainerInfo info : transitionList) {
        System.err.println("Attempting to insert - datanode_id: " + datanodeId + ", container_id: " + containerId +
            ", timestamp: " + info.getTimestamp() + ", container_state: " + info.getState() +
            ", bcsid: " + info.getBcsid() + ", error_message: " + info.getErrorMessage());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void createDatanodeContainerIndex() {
    String createIndexSQL = queries.get("CREATE_DATANODE_CONTAINER_INDEX");
    try (Connection connection = getConnection();
         Statement stmt = connection.createStatement()) {
      stmt.execute(createIndexSQL);
    } catch (SQLException e) {
      System.err.println("Error while creating the index: " + e.getMessage());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void insertLatestContainerLogData() {
    createContainerLogTable();
    String selectSQL = queries.get("SELECT_LATEST_CONTAINER_LOG");
    String insertSQL = queries.get("INSERT_CONTAINER_LOG");

    try (Connection connection = getConnection();
         PreparedStatement selectStmt = connection.prepareStatement(selectSQL);
         ResultSet resultSet = selectStmt.executeQuery();
         PreparedStatement insertStmt = connection.prepareStatement(insertSQL)) {
      int batchSize = 1000;
      int count = 0;
      while (resultSet.next()) {
        long datanodeId = resultSet.getLong("datanode_id");
        long containerId = resultSet.getLong("container_id");
        String containerState = resultSet.getString("container_state");
        long bcsid = resultSet.getLong("bcsid");

        insertStmt.setLong(1, datanodeId);
        insertStmt.setLong(2, containerId);
        insertStmt.setString(3, containerState);
        insertStmt.setLong(4, bcsid);
        insertStmt.addBatch();

        count++;

        if (count % batchSize == 0) {
          insertStmt.executeBatch();
        }
      }

      if (count % batchSize != 0) {
        insertStmt.executeBatch();
      }
    } catch (SQLException e) {
      System.err.println("Error while inserting data into ContainerLogTable: " + e.getMessage());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void dropTable(String tableName) throws SQLException {
    String dropTableSQL = queries.get("DROP_TABLE").replace("{table_name}", tableName);
    Connection connection = null;
    try {
      connection = getConnection();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    try (Statement stmt = connection.createStatement()) {
      stmt.executeUpdate(dropTableSQL);
    } catch (SQLException e) {
      System.err.println("Error while dropping table: " + e.getMessage());
    }
  }

  public void closeConnection(Connection connection) {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        System.err.println("Error while closing the connection: " + e.getMessage());
      }
    }
  }
}

