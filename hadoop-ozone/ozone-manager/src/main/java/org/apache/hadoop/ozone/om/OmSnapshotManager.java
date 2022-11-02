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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;

import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_KEY_NAME;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;


/**
 * This class is used to manage/create OM snapshots.
 */
public final class OmSnapshotManager {
  private final OzoneManager ozoneManager;
  private final LoadingCache<String, OmSnapshot> snapshotCache;

  private static final Logger LOG =
      LoggerFactory.getLogger(OmSnapshotManager.class);

  OmSnapshotManager(OzoneManager ozoneManager) {
    this.ozoneManager = ozoneManager;

    // size of lru cache
    int cacheSize = ozoneManager.getConfiguration().getInt(
        OzoneConfigKeys.OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE,
        OzoneConfigKeys.OZONE_OM_SNAPSHOT_CACHE_MAX_SIZE_DEFAULT);

    CacheLoader<String, OmSnapshot> loader;
    loader = new CacheLoader<String, OmSnapshot>() {
      @Override

      // load the snapshot into the cache if not already there
      @Nonnull
      public OmSnapshot load(@Nonnull String snapshotTableKey)
          throws IOException {
        SnapshotInfo snapshotInfo;
        // see if the snapshot exists
        snapshotInfo = getSnapshotInfo(snapshotTableKey);

        // read in the snapshot
        OzoneConfiguration conf = ozoneManager.getConfiguration();
        OMMetadataManager snapshotMetadataManager;

        // Create the snapshot metadata manager by finding the corresponding
        // RocksDB instance, creating an OmMetadataManagerImpl instance based on
        // that
        try {
          snapshotMetadataManager = OmMetadataManagerImpl
              .createSnapshotMetadataManager(
              conf, snapshotInfo.getCheckpointDirName());
        } catch (IOException e) {
          LOG.error("Failed to retrieve snapshot: {}, {}", snapshotTableKey, e);
          throw e;
        }

        // create the other manager instances based on snapshot metadataManager
        PrefixManagerImpl pm = new PrefixManagerImpl(snapshotMetadataManager,
            false);
        KeyManagerImpl km = new KeyManagerImpl(null,
            ozoneManager.getScmClient(), snapshotMetadataManager, conf,
            ozoneManager.getBlockTokenSecretManager(),
            ozoneManager.getKmsProvider(), ozoneManager.getPerfMetrics());

        return new OmSnapshot(km, pm, ozoneManager,
            snapshotInfo.getVolumeName(),
            snapshotInfo.getBucketName(),
            snapshotInfo.getName());
      }
    };

    RemovalListener<String, OmSnapshot> removalListener
        = notification -> {
          try {
            // close snapshot's rocksdb on eviction
            notification.getValue().close();
          } catch (IOException e) {
            LOG.error("Failed to close snapshot: {} {}",
                notification.getKey(), e);
          }
        };
    // init LRU cache
    snapshotCache = CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .removalListener(removalListener)
        .build(loader);
  }

  /**
   * Creates snapshot checkpoint that corresponds to snapshotInfo.
   * @param omMetadataManager the metadata manager
   * @param snapshotInfo The metadata of snapshot to be created
   * @return instance of DBCheckpoint
   */
  public static DBCheckpoint createOmSnapshotCheckpoint(
      OMMetadataManager omMetadataManager, SnapshotInfo snapshotInfo)
      throws IOException {
    RDBStore store = (RDBStore) omMetadataManager.getStore();

    final long dbLatestSequenceNumber = snapshotInfo.getDbTxSequenceNumber();

    final RocksDBCheckpointDiffer dbCpDiffer =
        omMetadataManager.getStore().getRocksDBCheckpointDiffer();

    final DBCheckpoint dbCheckpoint = store.getSnapshot(
        snapshotInfo.getCheckpointDirName());

    // Write snapshot generation (latest sequence number) to compaction log.
    // This will be used for DAG reconstruction as snapshotGeneration.
    dbCpDiffer.appendSequenceNumberToCompactionLog(dbLatestSequenceNumber);

    // Set compaction log filename to the latest DB sequence number
    // right after taking the RocksDB checkpoint for Ozone snapshot.
    //
    // Note it doesn't matter if sequence number hasn't increased (even though
    // it shouldn't happen), since the writer always appends the file.
    dbCpDiffer.setCurrentCompactionLog(dbLatestSequenceNumber);

    return dbCheckpoint;
  }

  // Get OmSnapshot if the keyname has ".snapshot" key indicator
  public IOmMetadataReader checkForSnapshot(String volumeName,
                                     String bucketName, String keyname)
      throws IOException {
    if (keyname == null) {
      return ozoneManager.getOmMetadataReader();
    }

    // see if key is for a snapshot
    String[] keyParts = keyname.split("/");
    if (isSnapshotKey(keyParts)) {
      String snapshotName = keyParts[1];
      if (snapshotName == null || snapshotName.isEmpty()) {
        // don't allow snapshot indicator without snapshot name
        throw new OMException(INVALID_KEY_NAME);
      }
      String snapshotTableKey = SnapshotInfo.getTableKey(volumeName,
          bucketName, snapshotName);

      // retrieve the snapshot from the cache
      try {
        return snapshotCache.get(snapshotTableKey);
      } catch (ExecutionException e) {
        throw new IOException(e.getCause());
      }
    } else {
      return ozoneManager.getOmMetadataReader();
    }
  }

  public SnapshotInfo getSnapshotInfo(String volumeName,
                                      String bucketName, String snapshotName)
      throws IOException {
    return getSnapshotInfo(SnapshotInfo.getTableKey(volumeName,
        bucketName, snapshotName));
  }

  private SnapshotInfo getSnapshotInfo(String key) throws IOException {
    SnapshotInfo snapshotInfo;
    try {
      snapshotInfo = ozoneManager.getMetadataManager()
        .getSnapshotInfoTable()
        .get(key);
    } catch (IOException e) {
      LOG.error("Snapshot {}: not found: {}", key, e);
      throw e;
    }
    if (snapshotInfo == null) {
      throw new OMException(KEY_NOT_FOUND);
    }
    return snapshotInfo;
  }

  public static String getSnapshotPrefix(String snapshotName) {
    return OM_SNAPSHOT_INDICATOR + OM_KEY_PREFIX +
        snapshotName + OM_KEY_PREFIX;
  }

  public static boolean isSnapshotKey(String[] keyParts) {
    return (keyParts.length > 1) &&
        (keyParts[0].compareTo(OM_SNAPSHOT_INDICATOR) == 0);
  }
}
