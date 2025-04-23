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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;


/**
 * Handles creation and interaction with the database.
 * Provides methods for table creation, log data insertion, and index setup.
 */

public class ContainerDatanodeDatabase {

  private static Map<String, String> queries;
  private static final int DEFAULT_REPLICATION_FACTOR ;
  static {
    OzoneConfiguration configuration = new OzoneConfiguration();
    final String replication = configuration.getTrimmed(
        OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_KEY,
        OMConfigKeys.OZONE_SERVER_DEFAULT_REPLICATION_DEFAULT);
    
    DEFAULT_REPLICATION_FACTOR = Integer.parseInt(replication.toUpperCase());
  }

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

  private void createIdxDclContainerStateTime(Connection conn) throws SQLException {
    String sql = queries.get("CREATE_DCL_CONTAINER_STATE_TIME_INDEX");
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
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
      createIdxDclContainerStateTime(connection);
      List<DatanodeContainerInfo> logEntries = getContainerLogData(containerID, connection);

      if (logEntries.isEmpty()) {
        System.out.println("Missing container with ID: " + containerID);
        return;
      }
      
      System.out.printf("%-25s | %-15s | %-35s | %-20s | %-10s | %-30s | %-12s%n",
          "Timestamp", "Container ID", "Datanode ID", "Container State", "BCSID", "Message", "Index Value");
      System.out.println("-----------------------------------------------------------------------------------" +
          "-------------------------------------------------------------------------------------------------");

      for (DatanodeContainerInfo entry : logEntries) {
        System.out.printf("%-25s | %-15d | %-35s | %-20s | %-10d | %-30s | %-12d%n",
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
        System.out.println("Container " + containerID + " might have duplicate OPEN state.");
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
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void analyzeContainerHealth(Long containerID,
                                      Map<String, DatanodeContainerInfo> latestPerDatanode) {

    Set<String> lifeCycleStates = new HashSet<>();
    for (HddsProtos.LifeCycleState state : HddsProtos.LifeCycleState.values()) {
      lifeCycleStates.add(state.name());
    }

    Set<String> healthStates = new HashSet<>();
    for (ReplicationManagerReport.HealthState state : ReplicationManagerReport.HealthState.values()) {
      healthStates.add(state.name());
    }

    Set<String> unhealthyReplicas = new HashSet<>();
    Set<String> closedReplicas = new HashSet<>();
    Set<String> openReplicas = new HashSet<>();
    Set<String> quasiclosedReplicas = new HashSet<>();
    Set<String> deletedReplicas = new HashSet<>();
    Set<Long> bcsids = new HashSet<>();
    Set<String> datanodeIds = new HashSet<>();
    List<String> unhealthyTimestamps = new ArrayList<>();
    List<String> closedTimestamps = new ArrayList<>();

    for (DatanodeContainerInfo entry : latestPerDatanode.values()) {
      String datanodeId = entry.getDatanodeId();
      String state = entry.getState();
      long bcsid = entry.getBcsid();
      String stateTimestamp = entry.getTimestamp();

      datanodeIds.add(datanodeId);



      if (healthStates.contains(state.toUpperCase())) {

        ReplicationManagerReport.HealthState healthState =
            ReplicationManagerReport.HealthState.valueOf(state.toUpperCase());

        if (healthState == ReplicationManagerReport.HealthState.UNHEALTHY) {
          unhealthyReplicas.add(datanodeId);
          unhealthyTimestamps.add(stateTimestamp);
        }
        
      } else if (lifeCycleStates.contains(state.toUpperCase())) {

        HddsProtos.LifeCycleState lifeCycleState = HddsProtos.LifeCycleState.valueOf(state.toUpperCase());

        switch (lifeCycleState) {
        case OPEN:
          openReplicas.add(datanodeId);
          break;
        case CLOSED:
          closedReplicas.add(datanodeId);
          bcsids.add(bcsid);
          closedTimestamps.add(stateTimestamp);
          break;
        case QUASI_CLOSED:
          quasiclosedReplicas.add(datanodeId);
          break;
        case DELETED:
          deletedReplicas.add(datanodeId);
          break;
        default:
          break;
        }

      }
    }

    int unhealthyCount = unhealthyReplicas.size();
    int replicaCount = datanodeIds.size();
    
    if (bcsids.size() > 1) {
      System.out.println("Container " + containerID + " has MISMATCHED REPLICATION as there are multiple" +
          " CLOSED containers with varying BCSIDs.");
    } else if (!closedReplicas.isEmpty()
        && closedReplicasCount < DEFAULT_REPLICATION_FACTOR) {
      System.out.println("Container " + containerID + " has fewer CLOSED replicas than required so container" +
          " is UNDER-REPLICATED.");
    } else if (!unhealthyReplicas.isEmpty()
        && closedReplicasCount == DEFAULT_REPLICATION_FACTOR) {

      String latestUnhealthy = Collections.max(unhealthyTimestamps);
      boolean allClosedNewer = closedTimestamps.stream()
          .allMatch(ct -> ct.compareTo(latestUnhealthy) > 0);

      if (allClosedNewer) {
        System.out.println("Container " + containerID + " has newer CLOSED replicas so its not UNHEALTHY.");
      }
    } else if (unhealthyCount == replicaCount && replicaCount >= DEFAULT_REPLICATION_FACTOR) {
      System.out.println("Container " + containerID + " is UNHEALTHY across all datanodes.");
    } else if (unhealthyCount >= 2 && closedReplicas.size() == replicaCount - unhealthyCount) {
      System.out.println("Container " + containerID + " is both UNHEALTHY and UNDER-REPLICATED.");
    } else if (unhealthyCount == 1 && closedReplicas.size() == replicaCount - unhealthyCount) {
      System.out.println("Container " + containerID + " is UNDER-REPLICATED.");
    } else if (openReplicas.size() > 0 &&
        (closedReplicas.size() > 0 || unhealthyCount > 0) &&
        (replicaCount - deletedReplicas.size()) >= DEFAULT_REPLICATION_FACTOR) {
      System.out.println("Container " + containerID + " is OPEN_UNHEALTHY.");
    } else if (quasiclosedReplicas.size() >= DEFAULT_REPLICATION_FACTOR) {
      System.out.println("Container " + containerID + " is QUASI_CLOSED_STUCK.");
    } else if (replicaCount - deletedReplicas.size() < DEFAULT_REPLICATION_FACTOR) {
      System.out.println("Container " + containerID + " is UNDER-REPLICATED.");
    } else if (replicaCount - deletedReplicas.size() > DEFAULT_REPLICATION_FACTOR) {
      System.out.println("Container " + containerID + " is OVER-REPLICATED.");
    } else {
      System.out.println("Container " + containerID + " has enough replicas.");
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
    String query = queries.get("CONTAINER_DETAILS_QUERY");
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
}

