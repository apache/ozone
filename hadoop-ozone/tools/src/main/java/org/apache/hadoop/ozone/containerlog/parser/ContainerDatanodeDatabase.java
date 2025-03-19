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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.sqlite.SQLiteConfig;

/**
 * Datanode container Database.
 */

public class ContainerDatanodeDatabase {

  private static Connection getConnection() throws Exception {
    Class.forName("org.sqlite.JDBC");

    SQLiteConfig config = new SQLiteConfig();

    config.setJournalMode(SQLiteConfig.JournalMode.OFF);
    config.setCacheSize(1000000);
    config.setLockingMode(SQLiteConfig.LockingMode.EXCLUSIVE);
    config.setSynchronous(SQLiteConfig.SynchronousMode.OFF);
    config.setTempStore(SQLiteConfig.TempStore.MEMORY);

    return DriverManager.getConnection("jdbc:sqlite:" + "container_datanode.db", config.toProperties());
  }


  public  void createDatanodeContainerLogTable() {
    String createTableSQL = "CREATE TABLE IF NOT EXISTS DatanodeContainerLogTable (" +
        "datanode_id INTEGER NOT NULL," +
        "container_id INTEGER NOT NULL," +
        "timestamp TEXT NOT NULL," +
        "container_state TEXT NOT NULL," +
        "bcsid INTEGER NOT NULL," +
        "error_message TEXT," +
        "PRIMARY KEY (datanode_id, container_id, container_state, bcsid)"  +
        ");";

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
    String createTableSQL = "CREATE TABLE IF NOT EXISTS ContainerLogTable (" +
        "datanode_id INTEGER NOT NULL," +
        "container_id INTEGER NOT NULL," +
        "latest_state TEXT NOT NULL," +
        "latest_bcsid INTEGER NOT NULL," +
        "PRIMARY KEY (datanode_id, container_id)" +
        ");";

    try (Connection connection = getConnection();
         Statement stmt = connection.createStatement()) {
      stmt.execute(createTableSQL);
    } catch (SQLException e) {
      System.err.println("Error while creating the table: " + e.getMessage());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public  void insertContainerDatanodeData(String key, List<DatanodeContainerInfo> transitionList) {


    String[] parts = key.split("#");
    if (parts.length != 2) {
      System.err.println("Invalid key format: " + key);
      return;
    }

    long containerId = Long.parseLong(parts[0]);
    long datanodeId = Long.parseLong(parts[1]);

    String insertSQL = "INSERT OR REPLACE INTO DatanodeContainerLogTable " +
        "(datanode_id, container_id, timestamp, container_state, bcsid, error_message) " +
        "VALUES (?, ?, ?, ?, ?, ?)";

    try (Connection connection = getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {

      int batchSize = 1000;
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

        if (count % batchSize == 0) {
          preparedStatement.executeBatch();
        }
      }

      if (count % batchSize != 0) {
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
    String createIndexSQL = "CREATE INDEX IF NOT EXISTS idx_datanode_container_timestamp " +
        "ON DatanodeContainerLogTable (datanode_id, container_id, timestamp);";

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

    String selectSQL = "SELECT datanode_id, container_id, container_state, bcsid, timestamp " +
        "FROM DatanodeContainerLogTable " +
        "WHERE (datanode_id, container_id, timestamp) IN (" +
        "  SELECT datanode_id, container_id, MAX(timestamp) " +
        "  FROM DatanodeContainerLogTable " +
        "  GROUP BY datanode_id, container_id" +
        ")";

    String insertSQL = "INSERT OR REPLACE INTO ContainerLogTable " +
        "(datanode_id, container_id, latest_state, latest_bcsid) " +
        "VALUES (?, ?, ?, ?)";

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
    String dropTableSQL = "DROP TABLE IF EXISTS " + tableName;
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

  public  void closeConnection(Connection connection) {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        System.err.println("Error while closing the connection: " + e.getMessage());
      }
    }
  }
}

