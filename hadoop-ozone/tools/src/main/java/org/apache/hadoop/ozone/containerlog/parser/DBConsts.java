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

/**
 * Constants used for ContainerDatanodeDatabase.
 */
public final class DBConsts {
  
  public static final String DEFAULT_DB_FILENAME = "container_datanode.db";
  public static final String DRIVER = "org.sqlite.JDBC";
  public static final String CONNECTION_PREFIX = "jdbc:sqlite:";
  public static final int CACHE_SIZE = 1000000;
  public static final int BATCH_SIZE = 2500;
  public static final String DATANODE_CONTAINER_LOG_TABLE_NAME = "DatanodeContainerLogTable";
  public static final String CONTAINER_LOG_TABLE_NAME = "ContainerLogTable";
  public static final String CREATE_DATANODE_CONTAINER_LOG_TABLE = 
      "CREATE TABLE IF NOT EXISTS DatanodeContainerLogTable (datanode_id TEXT NOT NULL, " +
          "container_id INTEGER NOT NULL, timestamp TEXT NOT NULL, container_state TEXT, bcsid INTEGER, " +
          "error_message TEXT, log_level TEXT NOT NULL," +
          " index_value INTEGER);";
  public static final String CREATE_CONTAINER_LOG_TABLE = 
      "CREATE TABLE IF NOT EXISTS ContainerLogTable (datanode_id TEXT NOT NULL, container_id INTEGER NOT NULL," +
          " latest_state TEXT, latest_bcsid INTEGER, PRIMARY KEY (datanode_id, container_id));";
  public static final String CREATE_DATANODE_CONTAINER_INDEX = 
      "CREATE INDEX IF NOT EXISTS idx_datanode_container ON DatanodeContainerLogTable (datanode_id," +
          " container_id, timestamp);";
  public static final String INSERT_DATANODE_CONTAINER_LOG = 
      "INSERT INTO DatanodeContainerLogTable (datanode_id, container_id, timestamp, container_state, bcsid," +
          " error_message, log_level, index_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?);";
  public static final String INSERT_CONTAINER_LOG = 
      "INSERT OR REPLACE INTO ContainerLogTable (datanode_id, container_id, latest_state," +
          " latest_bcsid) VALUES (?, ?, ?, ?);";
  public static final String SELECT_LATEST_CONTAINER_LOG = 
      "SELECT a.datanode_id, a.container_id, a.container_state, a.bcsid, a.timestamp FROM DatanodeContainerLogTable" +
          " AS a JOIN  (SELECT datanode_id, container_id, MAX(timestamp) as timestamp FROM DatanodeContainerLogTable" +
          " GROUP BY datanode_id, container_id) as b ON a.datanode_id = b.datanode_id AND " +
          "a.container_id = b.container_id AND a.timestamp=b.timestamp;";
  public static final String DROP_TABLE = "DROP TABLE IF EXISTS {table_name};";
  public static final String CREATE_INDEX_LATEST_STATE = 
      "CREATE INDEX IF NOT EXISTS idx_container_log_state ON ContainerLogTable(latest_state);";
  public static final String SELECT_LATEST_CONTAINER_LOGS_BY_STATE = 
      "SELECT cl.datanode_id, cl.container_id, cl.latest_state, cl.latest_bcsid, dcl.error_message, dcl.index_value," +
          " dcl.timestamp FROM ContainerLogTable cl LEFT JOIN DatanodeContainerLogTable dcl ON" +
          " cl.datanode_id = dcl.datanode_id AND cl.container_id = dcl.container_id AND cl.latest_bcsid = dcl.bcsid " +
          "AND cl.latest_state = dcl.container_state WHERE cl.latest_state = ? " +
          "AND dcl.timestamp = (SELECT MAX(timestamp) FROM DatanodeContainerLogTable sub_dcl " +
          "WHERE sub_dcl.datanode_id = cl.datanode_id AND" +
          " sub_dcl.container_id = cl.container_id AND sub_dcl.bcsid = cl.latest_bcsid" +
          " AND sub_dcl.container_state = cl.latest_state)";
  
  private DBConsts() {
    //Never constructed
  }
}
