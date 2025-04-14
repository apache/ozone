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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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

  public void showContainerDetails(Long containerID) {
    String query = queries.get("CONTAINER_SELECT_QUERY");

    try (Connection connection = getConnection();
         PreparedStatement statement = connection.prepareStatement(query)) {

      statement.setLong(1, containerID);

      if (checkForMultipleOpenStates(containerID, connection)) {
        return;
      }

      try (ResultSet resultSet = statement.executeQuery()) {

        boolean foundit = false;
        Set<Long> datanodeIds = new HashSet<>();
        Set<Long> unhealthyReplicas = new HashSet<>();
        Set<Long> closedReplicas = new HashSet<>();
        Set<Long> openReplicas = new HashSet<>();
        Set<Long> quasiclosedReplicas = new HashSet<>();
        Set<Long> bcsids = new HashSet<>();
        Set<Long> deletedReplicas = new HashSet<>();

        System.out.printf("%-12s | %-10s | %-10s | %-25s | %-30s | %-20s | %-12s%n",
            "Datanode ID", "State", "BCSID", "Timestamp", "Error Message", "Index Value", "Log Level");
        System.out.println("-------------------------------------------------------------------------" +
            "---------------------------------------------------------------------------------------------");

        while (resultSet.next()) {
          foundit = true;

          long datanodeId = resultSet.getLong("datanode_id");
          String latestState = resultSet.getString("latest_state");
          long latestBcsid = resultSet.getLong("latest_bcsid");
          String timestamp = resultSet.getString("timestamp");
          String errorMessage = resultSet.getString("error_message");
          int indexValue = resultSet.getInt("index_value");
          String logLevel = resultSet.getString("log_level");

          datanodeIds.add(datanodeId);

          if ("UNHEALTHY".equalsIgnoreCase(latestState)) {
            unhealthyReplicas.add(datanodeId);
          } else if ("CLOSED".equalsIgnoreCase(latestState)) {
            closedReplicas.add(datanodeId);
            bcsids.add(latestBcsid);
          } else if ("OPEN".equalsIgnoreCase(latestState)) {
            openReplicas.add(datanodeId);
          } else if ("QUASI_CLOSED".equalsIgnoreCase(latestState)) {
            quasiclosedReplicas.add(datanodeId);
          } else if ("DELETED".equalsIgnoreCase(latestState)) {
            deletedReplicas.add(datanodeId);
            bcsids.add(latestBcsid);
          }
          System.out.printf("%-12d | %-10s | %-10d | %-25s | %-30s | %-20s | %-12s%n",
              datanodeId, latestState, latestBcsid, timestamp, errorMessage, indexValue, logLevel);

        }

        if (!foundit) {
          System.out.println("Missing container with ID: " + containerID);
          return;
        }

        if (!deletedReplicas.isEmpty() && !closedReplicas.isEmpty() && bcsids.size() > 1) {
          System.out.println("Container " + containerID + " has MISMATCHED REPLICATION.");
          return;
        }

        int unhealthyCount = unhealthyReplicas.size();
        int replicaCount = datanodeIds.size();

        if (unhealthyCount == replicaCount) {
          System.out.println("Container " + containerID + " is UNHEALTHY across all datanodes.");
        } else if (unhealthyCount >= 2 && closedReplicas.size() == replicaCount - unhealthyCount) {
          System.out.println("Container " + containerID + " is both UNHEALTHY and UNDER-REPLICATED.");
        } else if (unhealthyCount == 1 && closedReplicas.size() == replicaCount - unhealthyCount) {
          System.out.println("Container " + containerID + " is UNDER-REPLICATED.");
        } else if ((!openReplicas.isEmpty() && openReplicas.size() < 3) &&
            (closedReplicas.size() == replicaCount - openReplicas.size() ||
                unhealthyCount == replicaCount - openReplicas.size())) {
          System.out.println("Container " + containerID + " is OPEN_UNHEALTHY.");
        } else if (quasiclosedReplicas.size() >= 3) {
          System.out.println("Container " + containerID + " is OUASI_CLOSED_STUCK.");
        } else {
          int expectedReplicationFactor = 3;
          if (replicaCount - deletedReplicas.size() < expectedReplicationFactor) {
            System.out.println("Container " + containerID + " is UNDER-REPLICATED.");
          } else if (replicaCount - deletedReplicas.size() > expectedReplicationFactor) {
            System.out.println("Container " + containerID + " is OVER-REPLICATED.");
          } else {
            System.out.println("Container " + containerID + " has enough replicas.");
          }
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private boolean checkForMultipleOpenStates(Long containerID, Connection connection) {
    String openCheckQuery = queries.get("OPEN_CHECK_QUERY");
    try (PreparedStatement openCheckStatement = connection.prepareStatement(openCheckQuery)) {
      openCheckStatement.setLong(1, containerID);

      try (ResultSet openCheckResult = openCheckStatement.executeQuery()) {
        List<String> firstOpenTimestamps = new ArrayList<>();
        Set<Long> firstOpenDatanodes = new HashSet<>();
        List<String[]> allRows = new ArrayList<>();
        boolean issueFound = false;

        while (openCheckResult.next()) {
          String timestamp = openCheckResult.getString("timestamp");
          long datanodeId = openCheckResult.getLong("datanode_id");
          String state = openCheckResult.getString("container_state");
          String errorMessage = openCheckResult.getString("error_message");
          long bcsid = openCheckResult.getLong("bcsid");
          int indexValue = openCheckResult.getInt("index_value");
          String logLevel = openCheckResult.getString("log_level");

          String[] row = new String[]{
              timestamp,
              String.valueOf(containerID),
              String.valueOf(datanodeId),
              state,
              String.valueOf(bcsid),
              errorMessage,
              String.valueOf(indexValue),
              logLevel
          };
          allRows.add(row);

          if ("OPEN".equalsIgnoreCase(state)) {
            if (firstOpenTimestamps.size() < 3 && !firstOpenDatanodes.contains(datanodeId)) {
              firstOpenTimestamps.add(timestamp);
              firstOpenDatanodes.add(datanodeId);
            } else if (isTimestampAfterFirstOpens(timestamp, firstOpenTimestamps)) {
              issueFound = true;
            }
          }
        }

        if (issueFound) {
          System.out.println("Issue found: Container " + containerID + " has duplicate OPEN state.");
          System.out.printf("%-25s | %-12s | %-12s | %-25s | %-12s | %-30s | %-12s | %-12s%n",
              "Timestamp", "Container ID", "Datanode ID", "Container State", "BCSID", "Error Message", "Index Value",
              "Log Level");
          System.out.println("-------------------------------------------------------------------------" +
              "---------------------------------------------------------------------------------------------");

          for (String[] row : allRows) {
            System.out.printf("%-25s | %-12s | %-12s | %-25s | %-12s | %-30s | %-12s | %-12s%n",
                row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]);
          }
          return true;
        } else {
          return false;
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
  }

  private boolean isTimestampAfterFirstOpens(String timestamp, List<String> firstOpenTimestamps) {
    for (String firstTimestamp : firstOpenTimestamps) {
      if (timestamp.compareTo(firstTimestamp) > 0) {
        return true;
      }
    }
    return false;
  }

}

