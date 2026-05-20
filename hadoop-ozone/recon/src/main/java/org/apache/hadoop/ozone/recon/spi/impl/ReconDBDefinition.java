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

package org.apache.hadoop.ozone.recon.spi.impl;

import java.util.Map;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.IntegerCodec;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.api.types.KeyPrefixContainer;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.codec.NSSummaryCodec;
import org.apache.hadoop.ozone.recon.scm.ContainerReplicaHistoryList;
import org.apache.hadoop.ozone.recon.tasks.FileSizeCountKey;
import org.apache.hadoop.ozone.recon.tasks.GlobalStatsValue;

/**
 * RocksDB definition for the DB internal to Recon.
 */
public class ReconDBDefinition extends DBDefinition.WithMap {

  private final String dbName;

  public static final DBColumnFamilyDefinition<ContainerKeyPrefix, Integer>
      CONTAINER_KEY =
      new DBColumnFamilyDefinition<>(
          "containerKeyTable",
          ContainerKeyPrefixCodec.get(),
          IntegerCodec.get());

  public static final DBColumnFamilyDefinition<KeyPrefixContainer, Integer>
      KEY_CONTAINER =
      new DBColumnFamilyDefinition<>(
          "keyContainerTable",
          KeyPrefixContainerCodec.get(),
          IntegerCodec.get());

  public static final DBColumnFamilyDefinition<Long, Long>
      CONTAINER_KEY_COUNT =
      new DBColumnFamilyDefinition<>(
          "containerKeyCountTable",
          LongCodec.get(),
          LongCodec.get());

  public static final DBColumnFamilyDefinition
      <Long, ContainerReplicaHistoryList> REPLICA_HISTORY =
      new DBColumnFamilyDefinition<Long, ContainerReplicaHistoryList>(
          "replica_history",
          LongCodec.get(),
          ContainerReplicaHistoryList.getCodec());

  public static final DBColumnFamilyDefinition<Long, NSSummary>
      NAMESPACE_SUMMARY = new DBColumnFamilyDefinition<Long, NSSummary>(
          "namespaceSummaryTable",
          LongCodec.get(),
          NSSummaryCodec.get());

  // Container Replica History with bcsId tracking.
  public static final DBColumnFamilyDefinition
      <Long, ContainerReplicaHistoryList> REPLICA_HISTORY_V2 =
      new DBColumnFamilyDefinition<Long, ContainerReplicaHistoryList>(
          "replica_history_v2",
          LongCodec.get(),
          ContainerReplicaHistoryList.getCodec());

  public static final DBColumnFamilyDefinition<FileSizeCountKey, Long>
      FILE_COUNT_BY_SIZE =
      new DBColumnFamilyDefinition<>(
          "fileCountBySizeTable",
          FileSizeCountKey.getCodec(),
          LongCodec.get());

  public static final DBColumnFamilyDefinition<String, GlobalStatsValue>
      GLOBAL_STATS =
      new DBColumnFamilyDefinition<>(
          "globalStatsTable",
          StringCodec.get(),
          GlobalStatsValue.getCodec());

  private static final Map<String, DBColumnFamilyDefinition<?, ?>>
      COLUMN_FAMILIES = DBColumnFamilyDefinition.newUnmodifiableMap(
          CONTAINER_KEY,
          CONTAINER_KEY_COUNT,
          KEY_CONTAINER,
          NAMESPACE_SUMMARY,
          REPLICA_HISTORY,
          REPLICA_HISTORY_V2,
          FILE_COUNT_BY_SIZE,
          GLOBAL_STATS);

  public ReconDBDefinition(String dbName) {
    super(COLUMN_FAMILIES);
    this.dbName = dbName;
  }

  @Override
  public String getName() {
    return dbName;
  }

  @Override
  public String getLocationConfigKey() {
    return ReconServerConfigKeys.OZONE_RECON_DB_DIR;
  }

}
