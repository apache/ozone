package org.apache.hadoop.ozone.om.snapshot.filter;

import com.google.common.collect.Lists;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.lock.MultiLocks;
import org.apache.hadoop.ozone.om.lock.OzoneManagerLock;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.util.CheckedExceptionOperation;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * This class is responsible for opening last N snapshot given snapshot or AOS metadata manager by acquiring a lock.
 */
public abstract class ReclaimableFilter<V> implements CheckedExceptionOperation<Table.KeyValue<String, V>,
    Boolean, IOException>, Closeable {

  private final OzoneManager ozoneManager;
  private final SnapshotInfo currentSnapshotInfo;
  private final OmSnapshotManager omSnapshotManager;
  private final SnapshotChainManager snapshotChainManager;

  private final List<SnapshotInfo> previousSnapshotInfos;
  private final List<ReferenceCounted<OmSnapshot>> previousOmSnapshots;
  private final MultiLocks<UUID> snapshotIdLocks;
  private Long volumeId;
  private OmBucketInfo bucketInfo;
  private final OMMetadataManager metadataManager;
  private final int numberOfPreviousSnapshotsFromChain;

  /**
   * Filter to return deleted keys/directories which are reclaimable based on their presence in previous snapshot in
   * the snapshot chain.
   *
   * @param omSnapshotManager
   * @param snapshotChainManager
   * @param currentSnapshotInfo  : If null the deleted keys in AOS needs to be processed, hence the latest snapshot
   *                             in the snapshot chain corresponding to bucket key needs to be processed.
   * @param metadataManager      : MetadataManager corresponding to snapshot or AOS.
   * @param lock                 : Lock for Active OM.
   */
  public ReclaimableFilter(OzoneManager ozoneManager, OmSnapshotManager omSnapshotManager,
                           SnapshotChainManager snapshotChainManager,
                           SnapshotInfo currentSnapshotInfo, OMMetadataManager metadataManager,
                           IOzoneManagerLock lock,
                           int numberOfPreviousSnapshotsFromChain) {
    this.ozoneManager = ozoneManager;
    this.omSnapshotManager = omSnapshotManager;
    this.currentSnapshotInfo = currentSnapshotInfo;
    this.snapshotChainManager = snapshotChainManager;
    this.snapshotIdLocks = new MultiLocks<>(lock, OzoneManagerLock.Resource.SNAPSHOT_GC_LOCK, false);
    this.metadataManager = metadataManager;
    this.numberOfPreviousSnapshotsFromChain = numberOfPreviousSnapshotsFromChain;
    this.previousOmSnapshots = new ArrayList<>(numberOfPreviousSnapshotsFromChain);
    this.previousSnapshotInfos = new ArrayList<>(numberOfPreviousSnapshotsFromChain);
  }

  private List<SnapshotInfo> getLastNSnapshotInChain(String volume, String bucket) throws IOException {
    if (currentSnapshotInfo != null &&
        (!currentSnapshotInfo.getVolumeName().equals(volume) || !currentSnapshotInfo.getBucketName().equals(bucket))) {
      throw new IOException("Volume & Bucket name for snapshot : " + currentSnapshotInfo + " not matching for " +
          "key in volume: " + volume + " bucket: " + bucket);
    }
    SnapshotInfo expectedPreviousSnapshotInfo = currentSnapshotInfo == null
        ? SnapshotUtils.getLatestSnapshotInfo(volume, bucket, ozoneManager, snapshotChainManager)
        : SnapshotUtils.getPreviousSnapshot(ozoneManager, snapshotChainManager, currentSnapshotInfo);
    List<SnapshotInfo> snapshotInfos = Lists.newArrayList(expectedPreviousSnapshotInfo);
    SnapshotInfo snapshotInfo = expectedPreviousSnapshotInfo;
    while (snapshotInfos.size() < numberOfPreviousSnapshotsFromChain) {
      snapshotInfo = snapshotInfo == null ? null
          : SnapshotUtils.getPreviousSnapshot(ozoneManager, snapshotChainManager, snapshotInfo);
      snapshotInfos.add(snapshotInfo);
      // If changes made to the snapshot have not been flushed to disk, throw exception immediately, next run of
      // garbage collection would process the snapshot.
      if (!OmSnapshotManager.areSnapshotChangesFlushedToDB(ozoneManager.getMetadataManager(), snapshotInfo)) {
        throw new IOException("Changes made to the snapshot " + snapshotInfo + " have not been flushed to the disk ");
      }
    }

    // Reversing list to get the correct order in chain. To ensure locking order is as per the chain ordering.
    Collections.reverse(snapshotInfos);
    return snapshotInfos;
  }

  private boolean validateExistingLastNSnapshotsInChain(String volume, String bucket) throws IOException {
    List<SnapshotInfo> expectedLastNSnapshotsInChain = getLastNSnapshotInChain(volume, bucket);
    List<UUID> expectedSnapshotIds = expectedLastNSnapshotsInChain.stream()
        .map(snapshotInfo -> snapshotInfo == null ? null : snapshotInfo.getSnapshotId())
        .collect(Collectors.toList());
    List<UUID> existingSnapshotIds = previousOmSnapshots.stream()
        .map(omSnapshotReferenceCounted -> omSnapshotReferenceCounted == null ? null :
            omSnapshotReferenceCounted.get().getSnapshotID()).collect(Collectors.toList());
    return expectedSnapshotIds.equals(existingSnapshotIds);
  }

  // Initialize the last N snapshots in the chain by acquiring locks. Throw IOException if it fails.
  private void initializePreviousSnapshotsFromChain(String volume, String bucket) throws IOException {
    // If existing snapshotIds don't match then close all snapshots and reopen the previous N snapshots.
    if (!validateExistingLastNSnapshotsInChain(volume, bucket)) {
      close();
      try {
        // Acquire lock only on last N-1 snapshot & current snapshot(AOS if it is null).
        List<SnapshotInfo> expectedLastNSnapshotsInChain = getLastNSnapshotInChain(volume, bucket);
        List<UUID> expectedSnapshotIds = expectedLastNSnapshotsInChain.stream()
            .map(snapshotInfo -> snapshotInfo == null ? null : snapshotInfo.getSnapshotId())
            .collect(Collectors.toList());
        List<UUID> lockIds = new ArrayList<>(expectedSnapshotIds.subList(1, expectedSnapshotIds.size()));
        lockIds.add(currentSnapshotInfo == null ? null : currentSnapshotInfo.getSnapshotId());

        if (snapshotIdLocks.acquireLock(lockIds).isLockAcquired()) {
          for (SnapshotInfo snapshotInfo : expectedLastNSnapshotsInChain) {
            if (snapshotInfo != null) {
              // For AOS fail operation if any of the previous snapshots are not active. currentSnapshotInfo for
              // AOS will be null.
              previousOmSnapshots.add(currentSnapshotInfo == null
                  ? omSnapshotManager.getActiveSnapshot(snapshotInfo.getVolumeName(), snapshotInfo.getBucketName(),
                  snapshotInfo.getName())
                  : omSnapshotManager.getSnapshot(snapshotInfo.getVolumeName(), snapshotInfo.getBucketName(),
                  snapshotInfo.getName()));
              previousSnapshotInfos.add(snapshotInfo);
            } else {
              previousOmSnapshots.add(null);
              previousSnapshotInfos.add(null);
            }

            // TODO: Getting volumeId and bucket from active OM. This would be wrong on volume & bucket renames
            //  support.
            volumeId = ozoneManager.getMetadataManager().getVolumeId(volume);
            String dbBucketKey = ozoneManager.getMetadataManager().getBucketKey(volume, bucket);
            bucketInfo = ozoneManager.getMetadataManager().getBucketTable().get(dbBucketKey);
          }
        } else {
          throw new IOException("Lock acquisition failed for last N snapshots : " +
              expectedLastNSnapshotsInChain + " " + currentSnapshotInfo);
        }
      } catch (IOException e) {
        this.close();
        throw e;
      }
    }
  }

  @Override
  public Boolean apply(Table.KeyValue<String, V> keyValue) throws IOException {
    String volume = getVolumeName(keyValue);
    String bucket = getBucketName(keyValue);
    initializePreviousSnapshotsFromChain(volume, bucket);
    boolean isReclaimable = isReclaimable(keyValue);
    // This is to ensure the reclamation ran on the same previous snapshot and no change occurred in the chain
    // while processing the entry.
    return isReclaimable && validateExistingLastNSnapshotsInChain(volume, bucket);
  }

  protected abstract String getVolumeName(Table.KeyValue<String, V> keyValue) throws IOException;

  protected abstract String getBucketName(Table.KeyValue<String, V> keyValue) throws IOException;

  protected abstract Boolean isReclaimable(Table.KeyValue<String, V> omKeyInfo) throws IOException;

  @Override
  public void close() throws IOException {
    this.snapshotIdLocks.releaseLock();
    for (ReferenceCounted<OmSnapshot> previousOmSnapshot : previousOmSnapshots) {
      previousOmSnapshot.close();
    }
    previousOmSnapshots.clear();
    previousSnapshotInfos.clear();
  }

  public ReferenceCounted<OmSnapshot> getPreviousOmSnapshot(int index) {
    return previousOmSnapshots.get(index);
  }

  public OMMetadataManager getMetadataManager() {
    return metadataManager;
  }

  public Long getVolumeId() {
    return volumeId;
  }

  public OmBucketInfo getBucketInfo() {
    return bucketInfo;
  }

  public SnapshotInfo getPreviousSnapshotInfo(int index) {
    return previousSnapshotInfos.get(index);
  }
}
