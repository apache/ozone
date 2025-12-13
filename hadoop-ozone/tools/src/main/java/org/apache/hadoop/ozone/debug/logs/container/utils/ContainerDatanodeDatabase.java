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

package org.apache.hadoop.ozone.debug.logs.container.utils;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.sqlite.SQLiteConfig;

/**
 * Handles creation and interaction with the database.
 * Provides methods for table creation, log data insertion, and index setup.
 */
public class ContainerDatanodeDatabase {
  
  private final String databasePath;
  private static final int DEFAULT_REPLICATION_FACTOR;

  private final PrintWriter out;
  private final PrintWriter err;

  public ContainerDatanodeDatabase(String dbPath) {
    this.databasePath = dbPath;
    this.out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
    this.err = new PrintWriter(new OutputStreamWriter(System.err, StandardCharsets.UTF_8), true);
  }

  public ContainerDatanodeDatabase(String dbPath, PrintWriter out, PrintWriter err) {
    this.databasePath = dbPath;
    this.out = out;
    this.err = err;
  }
  
  static {
    OzoneConfiguration configuration = new OzoneConfiguration();
    final String replication = configuration.getTrimmed(
        OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_KEY,
        OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_DEFAULT);

    DEFAULT_REPLICATION_FACTOR = Integer.parseInt(replication.toUpperCase());
  }
  
  private Connection getConnection() throws Exception {
    if (databasePath == null) {
      throw new IllegalStateException("Database path not set");
    }
    
    Class.forName(SQLDBConstants.DRIVER);

    SQLiteConfig config = new SQLiteConfig();

    config.setJournalMode(SQLiteConfig.JournalMode.OFF);
    config.setCacheSize(SQLDBConstants.CACHE_SIZE);
    config.setLockingMode(SQLiteConfig.LockingMode.EXCLUSIVE);
    config.setSynchronous(SQLiteConfig.SynchronousMode.OFF);
    config.setTempStore(SQLiteConfig.TempStore.MEMORY);

    return DriverManager.getConnection(SQLDBConstants.CONNECTION_PREFIX + databasePath, config.toProperties());
  }

  public void createDatanodeContainerLogTable() throws SQLException {
    String createTableSQL = SQLDBConstants.CREATE_DATANODE_CONTAINER_LOG_TABLE;
    try (Connection connection = getConnection();
         Statement dropStmt = connection.createStatement();
         Statement createStmt = connection.createStatement()) {
      dropTable(SQLDBConstants.DATANODE_CONTAINER_LOG_TABLE_NAME, dropStmt);
      createStmt.execute(createTableSQL);
      createDatanodeContainerIndex(createStmt);
    } catch (SQLException e) {
      throw new SQLException("Error while creating the table: " + e.getMessage());
    } catch (Exception e) {
      throw new RuntimeException("Unexpected error: "  + e);
    }
  }

  private void createContainerLogTable() throws SQLException {
    String createTableSQL = SQLDBConstants.CREATE_CONTAINER_LOG_TABLE;
    try (Connection connection = getConnection();
         Statement dropStmt = connection.createStatement();
         Statement createStmt = connection.createStatement()) {
      dropTable(SQLDBConstants.CONTAINER_LOG_TABLE_NAME, dropStmt);
      createStmt.execute(createTableSQL);
    } catch (SQLException e) {
      throw new SQLException("Error while creating the table: " + e.getMessage());
    } catch (Exception e) {
      throw new RuntimeException("Unexpected error: "  + e);
    }
  }

  public void createIndexes() throws SQLException {
    try (Connection connection = getConnection();
         Statement stmt = connection.createStatement()) {
      createIdxDclContainerStateTime(stmt);
      createContainerLogIndex(stmt);
      createIdxContainerlogContainerId(stmt);
      createIndexForQuasiClosedQuery(stmt);
    } catch (SQLException e) {
      throw new SQLException("Error while creating index: " + e.getMessage());
    } catch (Exception e) {
      throw new RuntimeException("Unexpected error: "  + e);
    }
  }

  private void createIdxDclContainerStateTime(Statement stmt) throws SQLException {
    String createIndexSQL = SQLDBConstants.CREATE_DCL_CONTAINER_STATE_TIME_INDEX;
    stmt.execute(createIndexSQL);
  }

  private void createContainerLogIndex(Statement stmt) throws SQLException {
    String createIndexSQL = SQLDBConstants.CREATE_INDEX_LATEST_STATE;
    stmt.execute(createIndexSQL);
  }

  private void createIdxContainerlogContainerId(Statement stmt) throws SQLException {
    String createIndexSQL = SQLDBConstants.CREATE_CONTAINER_ID_INDEX;
    stmt.execute(createIndexSQL);
  }

  private void createIndexForQuasiClosedQuery(Statement stmt) throws SQLException {
    String createIndexSQL = SQLDBConstants.CREATE_DCL_STATE_CONTAINER_DATANODE_TIME_INDEX;
    stmt.execute(createIndexSQL);
  }

  /**
   * Inserts a list of container log entries into the DatanodeContainerLogTable.
   *
   * @param transitionList List of container log entries to insert into the table.
   */
  
  public synchronized void insertContainerDatanodeData(List<DatanodeContainerInfo> transitionList) throws SQLException {

    String insertSQL = SQLDBConstants.INSERT_DATANODE_CONTAINER_LOG;

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

        if (count % SQLDBConstants.BATCH_SIZE == 0) {
          preparedStatement.executeBatch();
          count = 0;
        }
      }

      if (count != 0) {
        preparedStatement.executeBatch();
      }
    } catch (SQLException e) {
      throw new SQLException("Failed to insert container log for container " + containerId + 
              " on datanode " + datanodeId);
    } catch (Exception e) {
      throw new RuntimeException("Unexpected error: "  + e);
    }
  }

  private void createDatanodeContainerIndex(Statement stmt) throws SQLException {
    String createIndexSQL = SQLDBConstants.CREATE_DATANODE_CONTAINER_INDEX;
    stmt.execute(createIndexSQL);
  }

  /**
   * Extracts the latest container log data from the DatanodeContainerLogTable
   * and inserts it into ContainerLogTable.
   */

  public void insertLatestContainerLogData() throws SQLException {
    createContainerLogTable();
    String selectSQL = SQLDBConstants.SELECT_LATEST_CONTAINER_LOG;
    String insertSQL = SQLDBConstants.INSERT_CONTAINER_LOG;

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

          if (count % SQLDBConstants.BATCH_SIZE == 0) {
            insertStmt.executeBatch();
            count = 0;
          }
        } catch (SQLException e) {
          throw new SQLException("Failed to insert container log entry for container " + containerId + " on datanode "
                  + datanodeId);
        }
      }

      if (count != 0) {
        insertStmt.executeBatch();
      }
    } catch (SQLException e) {
      throw new SQLException("Failed to insert container log entry: " + e.getMessage());
    } catch (Exception e) {
      throw new RuntimeException("Unexpected error: "  + e);
    }
  }

  private void dropTable(String tableName, Statement stmt) throws SQLException {
    String dropTableSQL = SQLDBConstants.DROP_TABLE.replace("{table_name}", tableName);
    stmt.executeUpdate(dropTableSQL);
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

    String baseQuery = SQLDBConstants.SELECT_LATEST_CONTAINER_LOGS_BY_STATE;
    String finalQuery = limitProvided ? baseQuery + " LIMIT ?" : baseQuery;

    try (Connection connection = getConnection()) {

      try (PreparedStatement pstmt = connection.prepareStatement(finalQuery)) {
        pstmt.setString(1, state);
        if (limitProvided) {
          pstmt.setInt(2, limit + 1);
        }

        try (ResultSet rs = pstmt.executeQuery()) {

          out.printf("%-25s | %-35s | %-15s | %-15s | %-40s | %-12s%n",
              "Timestamp", "Datanode ID", "Container ID", "BCSID", "Message", "Index Value");
          out.println("-------------------------------------------------------------------------------------" +
              "---------------------------------------------------------------------------------------");

          while (rs.next()) {
            if (limitProvided && count >= limit) {
              out.println("Note: There might be more containers. Use -all option to list all entries");
              break;
            }
            String timestamp = rs.getString("timestamp");
            String datanodeId = rs.getString("datanode_id");
            long containerId = rs.getLong("container_id");
            long latestBcsid = rs.getLong("latest_bcsid");
            String errorMessage = rs.getString("error_message");
            int indexValue = rs.getInt("index_value");
            count++;

            out.printf("%-25s | %-35s | %-15d | %-15d | %-40s | %-12d%n",
                timestamp, datanodeId, containerId, latestBcsid, errorMessage, indexValue);
          }
          
          if (count == 0) {
            out.printf("No containers found for state: %s%n", state);
          } else {
            out.printf("Number of containers listed: %d%n", count);
          }
        }
      }
    } catch (SQLException e) {
      throw new SQLException("Error while retrieving containers with state " + state);
    } catch (Exception e) {
      throw new RuntimeException("Unexpected error: "  + e);
    }
  }

  /**
   * Displays detailed information about a container based on its ID, including its state, BCSID, 
   * timestamp, message, and index value. It also checks for issues such as UNHEALTHY 
   * replicas, under-replication, over-replication, OPEN_UNHEALTHY, OUASI_CLOSED_STUCK, mismatched replication
   * and duplicate open.
   *
   * @param containerID The ID of the container to display details for.
   */

  public void showContainerDetails(Long containerID) throws SQLException {

    try (Connection connection = getConnection()) {

      List<DatanodeContainerInfo> logEntries = getContainerLogData(containerID, connection);

      if (logEntries.isEmpty()) {
        out.println("Missing container with ID: " + containerID);
        return;
      }

      out.printf("%-25s | %-15s | %-35s | %-20s | %-10s | %-30s | %-12s%n",
          "Timestamp", "Container ID", "Datanode ID", "Container State", "BCSID", "Message", "Index Value");
      out.println("-----------------------------------------------------------------------------------" +
          "-------------------------------------------------------------------------------------------------");

      for (DatanodeContainerInfo entry : logEntries) {
        out.printf("%-25s | %-15d | %-35s | %-20s | %-10d | %-30s | %-12d%n",
            entry.getTimestamp(),
            entry.getContainerId(),
            entry.getDatanodeId(),
            entry.getState(),
            entry.getBcsid(),
            entry.getErrorMessage(),
            entry.getIndexValue());
      }

      logEntries.sort(Comparator.comparing(DatanodeContainerInfo::getTimestamp));

      if (checkForMultipleOpenStates(logEntries)) {
        out.println("Container " + containerID + " might have duplicate OPEN state.");
        return;
      }

      Map<String, DatanodeContainerInfo> latestPerDatanode = new HashMap<>();
      for (DatanodeContainerInfo entry : logEntries) {
        String datanodeId = entry.getDatanodeId();
        DatanodeContainerInfo existing = latestPerDatanode.get(datanodeId);
        if (existing == null || entry.getTimestamp().compareTo(existing.getTimestamp()) > 0) {
          latestPerDatanode.put(datanodeId, entry);
        }
      }

      analyzeContainerHealth(containerID, latestPerDatanode);

    } catch (SQLException e) {
      throw new SQLException("Error while retrieving container with ID " + containerID);
    } catch (Exception e) {
      throw new RuntimeException("Unexpected error: "  + e);
    }
  }

  private void analyzeContainerHealth(Long containerID, 
      Map<String, DatanodeContainerInfo> latestPerDatanode) {

    Set<String> lifeCycleStates = new HashSet<>();
    for (HddsProtos.LifeCycleState state : HddsProtos.LifeCycleState.values()) {
      lifeCycleStates.add(state.name());
    }

    Set<String> healthStates = new HashSet<>();
    for (ContainerHealthState state : ContainerHealthState.values()) {
      healthStates.add(state.name());
    }

    Set<String> unhealthyReplicas = new HashSet<>();
    Set<String> closedReplicas = new HashSet<>();
    Set<String> openReplicas = new HashSet<>();
    Set<String> quasiclosedReplicas = new HashSet<>();
    Set<String> deletedReplicas = new HashSet<>();
    Set<Long> bcsids = new HashSet<>();
    Set<String> datanodeIds = new HashSet<>();
    List<String> closedTimestamps = new ArrayList<>();
    List<String> otherTimestamps = new ArrayList<>();

    for (DatanodeContainerInfo entry : latestPerDatanode.values()) {
      String datanodeId = entry.getDatanodeId();
      String state = entry.getState();
      long bcsid = entry.getBcsid();
      String stateTimestamp = entry.getTimestamp();

      datanodeIds.add(datanodeId);
      
      if (healthStates.contains(state.toUpperCase())) {

        ContainerHealthState healthState =
            ContainerHealthState.valueOf(state.toUpperCase());

        if (healthState == ContainerHealthState.UNHEALTHY) {
          unhealthyReplicas.add(datanodeId);
        }

      } else if (lifeCycleStates.contains(state.toUpperCase())) {

        HddsProtos.LifeCycleState lifeCycleState = HddsProtos.LifeCycleState.valueOf(state.toUpperCase());

        switch (lifeCycleState) {
        case OPEN:
          openReplicas.add(datanodeId);
          otherTimestamps.add(stateTimestamp);
          break;
        case CLOSING:
          otherTimestamps.add(stateTimestamp);
          break;
        case CLOSED:
          closedReplicas.add(datanodeId);
          bcsids.add(bcsid);
          closedTimestamps.add(stateTimestamp);
          break;
        case QUASI_CLOSED:
          quasiclosedReplicas.add(datanodeId);
          otherTimestamps.add(stateTimestamp);
          break;
        case DELETED:
          deletedReplicas.add(datanodeId);
          break;
        default:
          break;
        }

      }
    }

    int closedCount = closedReplicas.size();
    
    boolean allClosedNewer = closedCount > 0 && closedTimestamps.stream()
        .allMatch(ct -> otherTimestamps.stream().allMatch(ot -> ct.compareTo(ot) > 0));
    
    if (bcsids.size() > 1) {
      out.println("Container " + containerID + " has MISMATCHED REPLICATION as there are multiple" +
          " CLOSED containers with varying BCSIDs.");
    } else if (closedCount == DEFAULT_REPLICATION_FACTOR && allClosedNewer) {
      out.println("Container " + containerID + " has enough replicas.");
    } else if (closedCount > DEFAULT_REPLICATION_FACTOR && allClosedNewer) {
      out.println("Container " + containerID + " is OVER-REPLICATED.");
    } else if (closedCount < DEFAULT_REPLICATION_FACTOR && closedCount != 0 && allClosedNewer) {
      out.println("Container " + containerID + " is UNDER-REPLICATED.");
    } else {
      int replicaCount = datanodeIds.size();

      if (!quasiclosedReplicas.isEmpty()
              && closedReplicas.isEmpty()
              && quasiclosedReplicas.size() >= DEFAULT_REPLICATION_FACTOR) {
        out.println("Container " + containerID + " might be QUASI_CLOSED_STUCK.");
      } else if (!unhealthyReplicas.isEmpty()) {
        out.println("Container " + containerID + " has UNHEALTHY replicas.");
      } else if (!openReplicas.isEmpty() &&
              (replicaCount - openReplicas.size()) > 0) {
        out.println("Container " + containerID + " might be OPEN_UNHEALTHY.");
      } else if (replicaCount - deletedReplicas.size() < DEFAULT_REPLICATION_FACTOR) {
        out.println("Container " + containerID + " is UNDER-REPLICATED.");
      } else if (replicaCount - deletedReplicas.size() > DEFAULT_REPLICATION_FACTOR) {
        out.println("Container " + containerID + " is OVER-REPLICATED.");
      } else {
        out.println("Container " + containerID + " has enough replicas.");
      } 
    }
  }

  /**
   * Checks whether the specified container has multiple "OPEN" states.
   * If multiple "OPEN" states are found, it returns {@code true}.
   *
   * @param entries The list of {@link DatanodeContainerInfo} entries to check for multiple "OPEN" states.
   * @return {@code true} if multiple "OPEN" states are found, {@code false} otherwise.
   */

  private boolean checkForMultipleOpenStates(List<DatanodeContainerInfo> entries) {
    List<String> firstOpenTimestamps = new ArrayList<>();
    Set<String> firstOpenDatanodes = new HashSet<>();
    boolean issueFound = false;

    for (DatanodeContainerInfo entry : entries) {
      if ("OPEN".equalsIgnoreCase(entry.getState())) {
        if (firstOpenTimestamps.size() < DEFAULT_REPLICATION_FACTOR &&
            !firstOpenDatanodes.contains(entry.getDatanodeId())) {
          firstOpenTimestamps.add(entry.getTimestamp());
          firstOpenDatanodes.add(entry.getDatanodeId());
        } else if (isTimestampAfterFirstOpens(entry.getTimestamp(), firstOpenTimestamps)) {
          issueFound = true;
        }
      }
    }
    return issueFound;
  }

  private boolean isTimestampAfterFirstOpens(String timestamp, List<String> firstOpenTimestamps) {
    for (String firstTimestamp : firstOpenTimestamps) {
      if (timestamp.compareTo(firstTimestamp) > 0) {
        return true;
      }
    }
    return false;
  }

  private List<DatanodeContainerInfo> getContainerLogData(Long containerID, Connection connection)
      throws SQLException {
    String query = SQLDBConstants.CONTAINER_DETAILS_QUERY;
    List<DatanodeContainerInfo> logEntries = new ArrayList<>();

    try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
      preparedStatement.setLong(1, containerID);
      try (ResultSet rs = preparedStatement.executeQuery()) {
        while (rs.next()) {
          DatanodeContainerInfo entry = new DatanodeContainerInfo.Builder()
              .setTimestamp(rs.getString("timestamp"))
              .setContainerId(rs.getLong("container_id"))
              .setDatanodeId(rs.getString("datanode_id"))
              .setState(rs.getString("container_state"))
              .setBcsid(rs.getLong("bcsid"))
              .setErrorMessage(rs.getString("error_message"))
              .setIndexValue(rs.getInt("index_value"))
              .build();

          logEntries.add(entry);
        }
      }
    }

    return logEntries;
  }

  public void findDuplicateOpenContainer() throws SQLException {
    String sql = SQLDBConstants.SELECT_DISTINCT_CONTAINER_IDS_QUERY;

    try (Connection connection = getConnection()) {

      try (PreparedStatement statement = connection.prepareStatement(sql);
           ResultSet resultSet = statement.executeQuery()) {
        int count = 0;

        while (resultSet.next()) {
          Long containerID = resultSet.getLong("container_id");
          List<DatanodeContainerInfo> logEntries = getContainerLogDataForOpenContainers(containerID, connection);
          boolean hasIssue = checkForMultipleOpenStates(logEntries);
          if (hasIssue) {
            int openStateCount = (int) logEntries.stream()
                .filter(entry -> "OPEN".equalsIgnoreCase(entry.getState()))
                .count();
            count++;
            out.println("Container ID: " + containerID + " - OPEN state count: " + openStateCount);
          }
        }

        out.println("Total containers that might have duplicate OPEN state : " + count);

      }
    } catch (SQLException e) {
      throw new SQLException("Error while retrieving containers." + e.getMessage(), e);
    } catch (Exception e) {
      throw new RuntimeException("Unexpected error: "  + e);
    }
  }

  private List<DatanodeContainerInfo> getContainerLogDataForOpenContainers(Long containerID, Connection connection)
      throws SQLException {
    String query = SQLDBConstants.SELECT_CONTAINER_DETAILS_OPEN_STATE;
    List<DatanodeContainerInfo> logEntries = new ArrayList<>();

    try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
      preparedStatement.setLong(1, containerID);
      try (ResultSet rs = preparedStatement.executeQuery()) {
        while (rs.next()) {
          DatanodeContainerInfo entry = new DatanodeContainerInfo.Builder()
              .setTimestamp(rs.getString("timestamp"))
              .setContainerId(rs.getLong("container_id"))
              .setDatanodeId(rs.getString("datanode_id"))
              .setState(rs.getString("container_state"))
              .build();
          logEntries.add(entry);
        }
      }
    }

    return logEntries;
  }

  /**
   * Lists containers that are over- or under-replicated also provides count of replicas.
   */
  
  public void listReplicatedContainers(String overOrUnder, Integer limit) throws SQLException {
    String operator;
    if ("OVER_REPLICATED".equalsIgnoreCase(overOrUnder)) {
      operator = ">";
    } else if ("UNDER_REPLICATED".equalsIgnoreCase(overOrUnder)) {
      operator = "<";
    } else {
      err.println("Invalid type. Use OVER_REPLICATED or UNDER_REPLICATED.");
      return;
    }
    
    String rawQuery = SQLDBConstants.SELECT_REPLICATED_CONTAINERS;

    if (!rawQuery.contains("{operator}")) {
      err.println("Query not defined correctly.");
      return;
    }

    String finalQuery = rawQuery.replace("{operator}", operator);

    boolean limitProvided = limit != Integer.MAX_VALUE;
    if (limitProvided) {
      finalQuery += " LIMIT ?";
    }

    try (Connection connection = getConnection();
         PreparedStatement pstmt = connection.prepareStatement(finalQuery)) {

      pstmt.setInt(1, DEFAULT_REPLICATION_FACTOR);
      
      if (limitProvided) {
        pstmt.setInt(2, limit + 1);
      }

      try (ResultSet rs = pstmt.executeQuery()) {
        int count = 0;

        while (rs.next()) {
          if (limitProvided && count >= limit) {
            err.println("Note: There might be more containers. Use --all option to list all entries.");
            break;
          }

          out.printf("Container ID = %s - Count = %d%n", rs.getLong("container_id"), 
                  rs.getInt("replica_count"));
          count++;
        }

        out.println("Number of containers listed: " + count);

      }

    } catch (SQLException e) {
      throw new SQLException("Error while retrieving containers." + e.getMessage(), e);
    } catch (Exception e) {
      throw new RuntimeException("Unexpected error: "  + e);
    }
  }

  /**
   * Lists containers that are UNHEALTHY also provides count of replicas which are in UNHEALTHY state.
   */

  public void listUnhealthyContainers(Integer limit) throws SQLException {
    
    String query = SQLDBConstants.SELECT_UNHEALTHY_CONTAINERS;

    boolean limitProvided = limit != Integer.MAX_VALUE;
    if (limitProvided) {
      query += " LIMIT ?";
    }

    try (Connection connection = getConnection();
         PreparedStatement stmt = connection.prepareStatement(query)) {

      if (limitProvided) {
        stmt.setInt(1, limit + 1);
      }

      try (ResultSet rs = stmt.executeQuery()) {
        int count = 0;

        while (rs.next()) {
          if (limitProvided && count >= limit) {
            err.println("Note: There might be more containers. Use --all option to list all entries.");
            break;
          }

          out.printf("Container ID = %s - Count = %d%n", rs.getString("container_id"), 
                  rs.getInt("unhealthy_replica_count"));
          count++;
        }

        out.println("Number of containers listed: " + count);
      }

    } catch (SQLException e) {
      throw new SQLException("Error while retrieving containers." + e.getMessage(), e);
    } catch (Exception e) {
      throw new RuntimeException("Unexpected error: "  + e);
    }
  }

  /**
   * Lists containers that are QUASI_CLOSED stuck also provides count of replicas which are in QUASI_CLOSED state.
   */

  public void listQuasiClosedStuckContainers(Integer limit) throws SQLException {
  
    String query =  SQLDBConstants.SELECT_QUASI_CLOSED_STUCK_CONTAINERS;

    boolean limitProvided = limit != Integer.MAX_VALUE;
    if (limitProvided) {
      query += " LIMIT ?";
    }

    try (Connection connection = getConnection();
         PreparedStatement statement = connection.prepareStatement(query)) {

      if (limitProvided) {
        statement.setInt(1, limit + 1);
      }

      try (ResultSet resultSet = statement.executeQuery()) {
        int count = 0;

        while (resultSet.next()) {
          if (limitProvided && count >= limit) {
            err.println("Note: There might be more containers. Use --all option to list all entries.");
            break;
          }

          out.printf("Container ID = %s - Count = %d%n", resultSet.getString("container_id"),
                  resultSet.getInt("quasi_closed_replica_count"));
          count++;
        }

        out.println("Number of containers listed: " + count);
      }

    } catch (SQLException e) {
      throw new SQLException("Error while retrieving containers." + e.getMessage(), e);
    } catch (Exception e) {
      throw new RuntimeException("Unexpected error: "  + e);
    }
  }
}

