/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om;

import com.google.common.collect.Sets;
import org.apache.hadoop.hdds.utils.Scheduler;
import org.apache.hadoop.hdds.utils.db.UuidCodec;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotProperties;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Class managing Snapshot Properties corresponding to a snapshot.
 */
public class SnapshotPropertiesManager {
  private final ManagedRocksDB db;
  private final ColumnFamilyHandle snapshotPropertiesCf;
  private final Map<UUID, SnapshotProperties> snapshotPropertiesMap;
  private final Set<UUID> purgedSnapshots;
  private final Scheduler scheduler;

  private final OMMetadataManager omMetadataManager;
  private static final String PURGED_SNAPSHOT_CLEANUP_SERVICE =
      "PurgedSnapshotCleanupService";


  public SnapshotPropertiesManager(OMMetadataManager omMetadataManager,
                                   ManagedRocksDB snapshotDB,
                                   ColumnFamilyHandle snapshotPropertiesCf,
                                   long cleanupInterval) throws IOException {
    this.db = snapshotDB;
    this.snapshotPropertiesCf = snapshotPropertiesCf;
    this.snapshotPropertiesMap = new ConcurrentHashMap<>();
    this.purgedSnapshots = Sets.newConcurrentHashSet();
    this.omMetadataManager = omMetadataManager;
    initValues();
    this.scheduler = new Scheduler(PURGED_SNAPSHOT_CLEANUP_SERVICE,
        true, 1);
    this.scheduler.scheduleWithFixedDelay(this::cleanup, cleanupInterval,
        cleanupInterval, TimeUnit.MILLISECONDS);
  }

  private void cleanup() {
    try {
      for (UUID purgedSnapshot : purgedSnapshots) {
        Optional<SnapshotProperties> snapshotProperties = getSnapshotProperties(purgedSnapshot);
        if (!snapshotProperties.isPresent()) {
          continue;
        }
        SnapshotInfo snapshotInfo =
            this.omMetadataManager.getSnapshotInfoTable().getSkipCache(snapshotProperties.get().getSnapshotTableKey());
        if (snapshotInfo == null) {
          deleteSnapshotProperties(purgedSnapshot);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void initValues() throws IOException {
    try (ManagedRocksIterator iterator = ManagedRocksIterator.managed(
        this.db.get().newIterator(this.snapshotPropertiesCf))) {
      iterator.get().seekToFirst();
      while (iterator.get().isValid()) {
        UUID key = UuidCodec.get().fromPersistedFormat(iterator.get().key());
        SnapshotProperties snapshotProperties =
            SnapshotProperties.getCodec().fromPersistedFormat(iterator.get().value());
        snapshotPropertiesMap.put(key, snapshotProperties);
        if (snapshotProperties.isSnapshotPurged()) {
          purgedSnapshots.add(key);
        }
      }
    }
  }

  /**
   * @param snapshotId UUID of snapshot
   * @return the snapshot property corresponding to snapshot if exists otherwise null value is returned.
   */
  public Optional<SnapshotProperties> getSnapshotProperties(UUID snapshotId) {
    return Optional.ofNullable(this.snapshotPropertiesMap.get(snapshotId));
  }

  /**
   * @param snapshotId UUID of snapshot
   * @param snapshotProperties SnapshotProperties object corresponding to snapshot
   * @return the snapshot property corresponding to snapshot if exists otherwise null value is returned.
   */
  public void setSnapshotProperties(UUID snapshotId,
                                    SnapshotProperties snapshotProperties) throws IOException {
    try {
      this.db.get().put(this.snapshotPropertiesCf, UuidCodec.get().toPersistedFormat(snapshotId),
          SnapshotProperties.getCodec()
              .toPersistedFormat(snapshotProperties));
      this.snapshotPropertiesMap.put(snapshotId, snapshotProperties);
      if (snapshotProperties.isSnapshotPurged()) {
        purgedSnapshots.add(snapshotId);
      }
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  public void deleteSnapshotProperties(UUID snapshotId) throws IOException {
    try {
      this.db.get().delete(this.snapshotPropertiesCf, UuidCodec.get().toPersistedFormat(snapshotId));
      this.snapshotPropertiesMap.remove(snapshotId);
      this.purgedSnapshots.remove(snapshotId);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }


}
