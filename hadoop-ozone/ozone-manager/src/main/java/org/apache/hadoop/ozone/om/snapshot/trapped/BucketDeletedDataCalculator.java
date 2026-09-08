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

package org.apache.hadoop.ozone.om.snapshot.trapped;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableDirFilter;
import org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableKeyFilter;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;

/**
 * Computes bucket-level deleted bytes split into purgeable and snapshot-trapped.
 * <p>
 * This calculator follows KDS/DDS reclaimability semantics across active OM DB
 * and all active snapshots in the bucket chain.
 */
public class BucketDeletedDataCalculator {

  private final OzoneManager ozoneManager;
  private final OmSnapshotManager omSnapshotManager;
  private final SnapshotChainManager snapshotChainManager;

  public BucketDeletedDataCalculator(OzoneManager ozoneManager) {
    this.ozoneManager = ozoneManager;
    this.omSnapshotManager = ozoneManager.getOmSnapshotManager();
    this.snapshotChainManager =
        ((OmMetadataManagerImpl) ozoneManager.getMetadataManager())
            .getSnapshotChainManager();
  }

  public BucketDeletedBytesStats calculate(String volume, String bucket)
      throws IOException {
    BucketDeletedBytesStats totals = new BucketDeletedBytesStats();
    processStore(volume, bucket, null,
        requireKeyManager(ozoneManager.getKeyManager(), "active store"), totals);

    Iterator<UUID> iterator = snapshotChainManager.iterator(true);
    while (iterator.hasNext()) {
      UUID snapshotId = iterator.next();
      SnapshotInfo snapshotInfo = SnapshotUtils.getSnapshotInfo(
          ozoneManager, snapshotChainManager, snapshotId);
      if (snapshotInfo == null
          || snapshotInfo.getSnapshotStatus() != SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE
          || !snapshotInfo.getVolumeName().equals(volume)
          || !snapshotInfo.getBucketName().equals(bucket)
          || !OmSnapshotManager.areSnapshotChangesFlushedToDB(
              ozoneManager.getMetadataManager(), snapshotInfo)) {
        continue;
      }
      try (UncheckedAutoCloseableSupplier<OmSnapshot> snapshot =
               omSnapshotManager.getActiveSnapshot(volume, bucket, snapshotInfo.getName())) {
        processStore(volume, bucket, snapshotInfo,
            requireKeyManager(snapshot.get().getKeyManager(),
                "snapshot " + snapshotInfo.getTableKey()),
            totals);
      }
    }
    return totals;
  }

  private static KeyManager requireKeyManager(KeyManager keyManager, String scope)
      throws IOException {
    if (keyManager == null) {
      throw new IOException("KeyManager is not available for " + scope
          + ". Ensure OM is fully initialized before running the calculation.");
    }
    return keyManager;
  }

  private void processStore(
      String volume,
      String bucket,
      SnapshotInfo currentSnapshotInfo,
      KeyManager keyManager,
      BucketDeletedBytesStats totals) throws IOException {
    try (ReclaimableKeyFilter reclaimableKeyFilter = new ReclaimableKeyFilter(
             ozoneManager,
             omSnapshotManager,
             snapshotChainManager,
             currentSnapshotInfo,
             keyManager,
             ozoneManager.getMetadataManager().getLock());
         ReclaimableDirFilter reclaimableDirFilter = new ReclaimableDirFilter(
             ozoneManager,
             omSnapshotManager,
             snapshotChainManager,
             currentSnapshotInfo,
             keyManager,
             ozoneManager.getMetadataManager().getLock())) {

      processDeletedKeys(volume, bucket, keyManager, reclaimableKeyFilter, totals);
      processDeletedDirs(volume, bucket, keyManager, reclaimableKeyFilter,
          reclaimableDirFilter, totals);
    }
  }

  private void processDeletedKeys(
      String volume,
      String bucket,
      KeyManager keyManager,
      ReclaimableKeyFilter reclaimableKeyFilter,
      BucketDeletedBytesStats totals) throws IOException {
    OMMetadataManager metadataManager = keyManager.getMetadataManager();
    Table<String, RepeatedOmKeyInfo> deletedTable = metadataManager.getDeletedTable();
    String deletedTablePrefix = metadataManager.getTableBucketPrefix(
        deletedTable.getName(), volume, bucket);
    try (TableIterator<String, ? extends KeyValue<String, RepeatedOmKeyInfo>> itr =
             deletedTable.iterator(deletedTablePrefix)) {
      while (itr.hasNext()) {
        KeyValue<String, ? extends RepeatedOmKeyInfo> entry = itr.next();
        for (OmKeyInfo omKeyInfo : entry.getValue().getOmKeyInfoList()) {
          if (reclaimableKeyFilter.apply(Table.newKeyValue(entry.getKey(), omKeyInfo))) {
            totals.addPurgeableKey(omKeyInfo.getReplicatedSize());
          } else {
            totals.addSnapshotTrappedKey(omKeyInfo.getReplicatedSize());
          }
        }
      }
    }
  }

  private void processDeletedDirs(
      String volume,
      String bucket,
      KeyManager keyManager,
      ReclaimableKeyFilter reclaimableKeyFilter,
      ReclaimableDirFilter reclaimableDirFilter,
      BucketDeletedBytesStats totals) throws IOException {
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    long volumeId = metadataManager.getVolumeId(volume);
    long bucketId = metadataManager.getBucketId(volume, bucket);
    KeyManager traversalKeyManager = ozoneManager.getKeyManager();

    try (TableIterator<String, Table.KeyValue<String, OmKeyInfo>> deletedDirItr =
             keyManager.getDeletedDirEntries(volume, bucket)) {
      while (deletedDirItr.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> rootEntry = deletedDirItr.next();
        traverseDeletedDirSubtree(
            traversalKeyManager, reclaimableKeyFilter, reclaimableDirFilter,
            volumeId, bucketId, rootEntry, totals);
      }
    }
  }

  private void traverseDeletedDirSubtree(
      KeyManager keyManager,
      ReclaimableKeyFilter reclaimableKeyFilter,
      ReclaimableDirFilter reclaimableDirFilter,
      long volumeId,
      long bucketId,
      Table.KeyValue<String, OmKeyInfo> rootEntry,
      BucketDeletedBytesStats totals) throws IOException {
    Queue<OmKeyInfo> queue = new ArrayDeque<>();
    Set<Long> visitedDirIds = new HashSet<>();
    Set<Long> visitedFileIds = new HashSet<>();

    queue.add(rootEntry.getValue());
    visitedDirIds.add(rootEntry.getValue().getObjectID());

    while (!queue.isEmpty()) {
      OmKeyInfo currentDir = queue.poll();
      boolean currentDirReclaimable =
          reclaimableDirFilter.apply(Table.newKeyValue("", currentDir));
      if (currentDirReclaimable) {
        totals.addPurgeableDir();
      } else {
        totals.addSnapshotTrappedDir();
      }

      for (OmKeyInfo subFile : keyManager.getPendingDeletionSubFiles(
          volumeId, bucketId, currentDir, keyValue -> true, Integer.MAX_VALUE).getKeysToDelete()) {
        if (!visitedFileIds.add(subFile.getObjectID())) {
          continue;
        }
        boolean fileReclaimable = currentDirReclaimable
            || reclaimableKeyFilter.apply(Table.newKeyValue("", subFile));
        if (fileReclaimable) {
          totals.addPurgeableKey(subFile.getReplicatedSize());
        } else {
          totals.addSnapshotTrappedKey(subFile.getReplicatedSize());
        }
      }

      for (OmKeyInfo subDir : keyManager.getPendingDeletionSubDirs(
          volumeId, bucketId, currentDir, keyValue -> true, Integer.MAX_VALUE).getKeysToDelete()) {
        if (visitedDirIds.add(subDir.getObjectID())) {
          queue.add(subDir);
        }
      }
    }
  }

  /**
   * Aggregated on-demand deleted-bytes view for one bucket.
   */
  public static final class BucketDeletedBytesStats {
    private long snapshotTrappedBytes;
    private long purgeableBytes;
    private long snapshotTrappedKeys;
    private long purgeableKeys;
    private long snapshotTrappedDirs;
    private long purgeableDirs;

    public long getSnapshotTrappedBytes() {
      return snapshotTrappedBytes;
    }

    public long getPurgeableBytes() {
      return purgeableBytes;
    }

    public long getSnapshotTrappedKeys() {
      return snapshotTrappedKeys;
    }

    public long getPurgeableKeys() {
      return purgeableKeys;
    }

    public long getSnapshotTrappedDirs() {
      return snapshotTrappedDirs;
    }

    public long getPurgeableDirs() {
      return purgeableDirs;
    }

    public void addSnapshotTrappedBytes(long value) {
      snapshotTrappedBytes += value;
    }

    public void addPurgeableBytes(long value) {
      purgeableBytes += value;
    }

    public void addSnapshotTrappedKey(long bytes) {
      addSnapshotTrappedBytes(bytes);
      snapshotTrappedKeys++;
    }

    public void addPurgeableKey(long bytes) {
      addPurgeableBytes(bytes);
      purgeableKeys++;
    }

    public void addSnapshotTrappedDir() {
      snapshotTrappedDirs++;
    }

    public void addPurgeableDir() {
      purgeableDirs++;
    }
  }
}

