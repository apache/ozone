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

import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_CONTAINER_KEY_DB;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.ProvisionException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import javax.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider for Recon's RDB.
 */
public class ReconDBProvider {
  private static final String STAGED_EXT = ".staged";
  private static final String BACKUP_EXT = ".backup";
  private OzoneConfiguration configuration;
  private ReconUtils reconUtils;
  private DBStore dbStore;

  @VisibleForTesting
  private static final Logger LOG =
          LoggerFactory.getLogger(ReconDBProvider.class);

  @Inject
  ReconDBProvider(OzoneConfiguration configuration, ReconUtils reconUtils) {
    this.configuration = configuration;
    this.reconUtils = reconUtils;
    this.dbStore = provideReconDB();
  }

  private ReconDBProvider(OzoneConfiguration configuration, ReconUtils reconUtils, DBStore dbStore) {
    this.configuration = configuration;
    this.reconUtils = reconUtils;
    this.dbStore = dbStore;
  }

  public ReconDBProvider getStagedReconDBProvider() throws IOException {
    File reconDbDir = reconUtils.getReconDbDir(configuration, OZONE_RECON_DB_DIR);
    String stagedDbName = RECON_CONTAINER_KEY_DB + STAGED_EXT;
    FileUtils.deleteDirectory(new File(reconDbDir, stagedDbName));
    DBStore db = initializeDBStore(configuration, stagedDbName);
    if (db == null) {
      throw new ProvisionException("Unable to initialize staged recon container DBStore");
    }
    return new ReconDBProvider(configuration, reconUtils, db);
  }

  public DBStore provideReconDB() {
    DBStore db;
    try {
      // handle recover of last known container as old format removing timestamp
      File reconDbDir = reconUtils.getReconDbDir(configuration, OZONE_RECON_DB_DIR);
      File lastKnownContainerKeyDb = reconUtils.getLastKnownDB(reconDbDir, RECON_CONTAINER_KEY_DB);
      if (lastKnownContainerKeyDb != null) {
        LOG.info("Last known Recon DB : {}", lastKnownContainerKeyDb.getAbsolutePath());
        Files.move(lastKnownContainerKeyDb.toPath(), new File(reconDbDir, RECON_CONTAINER_KEY_DB).toPath(),
            StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      }
      // if db does not exist, check if a backup exists and restore it. This can happen when replace with
      // staged db fails and backup is not restored at that point of time.
      if (!new File(reconDbDir, RECON_CONTAINER_KEY_DB).exists() &&
          new File(reconDbDir, RECON_CONTAINER_KEY_DB + BACKUP_EXT).exists()) {
        LOG.info("Recon DB backup found, restoring from backup.");
        Files.move(new File(reconDbDir, RECON_CONTAINER_KEY_DB + BACKUP_EXT).toPath(),
            new File(reconDbDir, RECON_CONTAINER_KEY_DB).toPath(),
            StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (IOException e) {
      throw new ProvisionException("Unable to recover container DB path.", e);
    }
    db = initializeDBStore(configuration, RECON_CONTAINER_KEY_DB);
    if (db == null) {
      throw new ProvisionException("Unable to provide instance of DBStore " +
              "store.");
    }
    return db;
  }

  public DBStore getDbStore() {
    return dbStore;
  }

  static void truncateTable(Table table) throws IOException {
    if (table == null) {
      return;
    }
    try (TableIterator<Object, ? extends KeyValue<Object, Object>>
            tableIterator = table.iterator()) {
      while (tableIterator.hasNext()) {
        KeyValue<Object, Object> entry = tableIterator.next();
        table.delete(entry.getKey());
      }
    } catch (Exception e) {
      // Check if this is a closed database exception
      if (e.getMessage() != null && e.getMessage().contains("closed")) {
        // Log warning but don't fail if database is already closed
        // This can happen during test cleanup or concurrent access scenarios
        LOG.warn("Cannot truncate table {} - database is closed. " +
            "Table may already be cleared or in process of being reinitialized.",
            table.getName());
        return;
      }
      // Re-throw other exceptions as they indicate real problems
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException("Failed to truncate table " + table.getName(), e);
    }
  }

  private static DBStore initializeDBStore(OzoneConfiguration configuration,
                                           String dbName) {
    DBStore dbStore = null;
    try {
      dbStore = DBStoreBuilder.createDBStore(configuration,
              new ReconDBDefinition(dbName));
    } catch (Exception ex) {
      LOG.error("Unable to initialize Recon container metadata store.", ex);
    }
    return dbStore;
  }

  public void close() throws Exception {
    if (this.dbStore != null) {
      dbStore.close();
      dbStore = null;
    }
  }

  public void replaceStagedDb(ReconDBProvider stagedReconDBProvider) throws Exception {
    File dbPath = dbStore.getDbLocation();
    File stagedDbPath = stagedReconDBProvider.getDbStore().getDbLocation();
    File backupPath = new File(dbPath.getAbsolutePath() + BACKUP_EXT);
    stagedReconDBProvider.close();
    try {
      FileUtils.deleteDirectory(backupPath);
      close();
      Files.move(dbPath.toPath(), backupPath.toPath(), StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
      Files.move(stagedDbPath.toPath(), dbPath.toPath(), StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
      dbStore = initializeDBStore(configuration, dbPath.getName());
    } catch (Exception e) {
      if (dbStore == null) {
        Files.move(dbPath.toPath(), stagedDbPath.toPath(), StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
        Files.move(backupPath.toPath(), dbPath.toPath(), StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
        dbStore = initializeDBStore(configuration, dbPath.getName());
      }
      throw e;
    }
  }
}
