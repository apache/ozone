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

import static org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition.REPLICA_HISTORY_V2;
import static org.apache.hadoop.ozone.recon.spi.impl.ReconReplicaHistoryDBDefinition.METADATA;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.scm.ContainerReplicaHistoryList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the replica history DB, which is never replaced by an OM or SCM snapshot.
 */
@Singleton
public class ReconReplicaHistoryDBProvider implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ReconReplicaHistoryDBProvider.class);
  private static final String MIGRATION_COMPLETE = "replicaHistoryMigrated";
  private final DBStore dbStore;
  private final Table<Long, ContainerReplicaHistoryList> replicaHistoryTable;

  @Inject
  public ReconReplicaHistoryDBProvider(OzoneConfiguration configuration) throws IOException {
    dbStore = DBStoreBuilder.createDBStore(configuration, new ReconReplicaHistoryDBDefinition());
    try {
      replicaHistoryTable = REPLICA_HISTORY_V2.getTable(dbStore);
    } catch (IOException | RuntimeException e) {
      try {
        dbStore.close();
      } catch (RuntimeException closeException) {
        e.addSuppressed(closeException);
      }
      throw e;
    }
  }

  /**
   * Migrate legacy history before services start. Safe to retry if version persistence fails after the copy.
   */
  public void migrateReplicaHistory(DBStore legacyStore) throws IOException {
    Table<String, String> metadata = METADATA.getTable(dbStore);
    File migrationFile = new File(dbStore.getDbLocation(), "replica-history-migration.sst");
    try {
      if (metadata.isExist(MIGRATION_COMPLETE)) {
        return;
      }
      // Retry incomplete migrations from the unchanged source before any services start.
      Files.deleteIfExists(migrationFile.toPath());
      legacyStore.getTable(REPLICA_HISTORY_V2.getName()).dumpToFileWithPrefix(migrationFile, null);
      dbStore.getTable(REPLICA_HISTORY_V2.getName()).loadFromFile(migrationFile);
      metadata.put(MIGRATION_COMPLETE, "true");
      dbStore.flushLog(true);
      LOG.info("Migrated container replica history to {}", dbStore.getDbLocation());
    } finally {
      if (migrationFile.exists() && !migrationFile.delete()) {
        LOG.warn("Unable to delete replica history migration file {}", migrationFile);
      }
    }
  }

  public DBStore getDbStore() {
    return dbStore;
  }

  public Table<Long, ContainerReplicaHistoryList> getReplicaHistoryTable() {
    return replicaHistoryTable;
  }

  @Override
  public void close() throws IOException {
    dbStore.close();
  }
}
