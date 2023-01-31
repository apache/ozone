/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.recon.spi.impl;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.IntegerCodec;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.api.types.KeyPrefixContainer;
import org.apache.hadoop.ozone.recon.codec.ContainerReplicaHistoryListCodec;
import org.apache.hadoop.ozone.recon.codec.NSSummaryCodec;
import org.apache.hadoop.ozone.recon.scm.ContainerReplicaHistoryList;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;

/**
 * RocksDB definition for the DB internal to Recon.
 */
public class ReconDBDefinition implements DBDefinition {

  private String dbName;

  public ReconDBDefinition(String dbName) {
    this.dbName = dbName;
  }

  public static final DBColumnFamilyDefinition<ContainerKeyPrefix, Integer>
      CONTAINER_KEY =
      new DBColumnFamilyDefinition<>(
          "containerKeyTable",
          ContainerKeyPrefix.class,
          new ContainerKeyPrefixCodec(),
          Integer.class,
          new IntegerCodec());

  public static final DBColumnFamilyDefinition<KeyPrefixContainer, Integer>
      KEY_CONTAINER =
      new DBColumnFamilyDefinition<>(
          "keyContainerTable",
          KeyPrefixContainer.class,
          new KeyPrefixContainerCodec(),
          Integer.class,
          new IntegerCodec());

  public static final DBColumnFamilyDefinition<Long, Long>
      CONTAINER_KEY_COUNT =
      new DBColumnFamilyDefinition<>(
          "containerKeyCountTable",
          Long.class,
          new LongCodec(),
          Long.class,
          new LongCodec());

  public static final DBColumnFamilyDefinition
      <Long, ContainerReplicaHistoryList> REPLICA_HISTORY =
      new DBColumnFamilyDefinition<Long, ContainerReplicaHistoryList>(
          "replica_history",
          Long.class,
          new LongCodec(),
          ContainerReplicaHistoryList.class,
          new ContainerReplicaHistoryListCodec());

  public static final DBColumnFamilyDefinition<Long, NSSummary>
      NAMESPACE_SUMMARY = new DBColumnFamilyDefinition<Long, NSSummary>(
          "namespaceSummaryTable",
          Long.class,
          new LongCodec(),
          NSSummary.class,
          new NSSummaryCodec());

  // Container Replica History with bcsId tracking.
  public static final DBColumnFamilyDefinition
      <Long, ContainerReplicaHistoryList> REPLICA_HISTORY_V2 =
      new DBColumnFamilyDefinition<Long, ContainerReplicaHistoryList>(
          "replica_history_v2",
          Long.class,
          new LongCodec(),
          ContainerReplicaHistoryList.class,
          new ContainerReplicaHistoryListCodec());

  @Override
  public String getName() {
    return dbName;
  }

  @Override
  public String getLocationConfigKey() {
    return ReconServerConfigKeys.OZONE_RECON_DB_DIR;
  }

  @Override
  public DBColumnFamilyDefinition[] getColumnFamilies() {
    return new DBColumnFamilyDefinition[] {
        CONTAINER_KEY, KEY_CONTAINER, CONTAINER_KEY_COUNT, REPLICA_HISTORY,
        NAMESPACE_SUMMARY, REPLICA_HISTORY_V2};
  }
}
