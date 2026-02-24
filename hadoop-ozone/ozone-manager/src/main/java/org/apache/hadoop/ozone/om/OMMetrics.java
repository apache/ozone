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

package org.apache.hadoop.ozone.om;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.utils.DBCheckpointMetrics;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;

/**
 * This class is for maintaining Ozone Manager statistics.
 */
@InterfaceAudience.Private
@Metrics(about = "Ozone Manager Metrics", context = "dfs")
public class OMMetrics implements OmMetadataReaderMetrics {
  private static final String SOURCE_NAME =
      OMMetrics.class.getSimpleName();

  // OM request type op metrics
  private @Metric MutableCounterLong numVolumeOps;
  private @Metric MutableCounterLong numBucketOps;
  private @Metric MutableCounterLong numKeyOps;
  private @Metric MutableCounterLong numFSOps;

  // OM op metrics
  private @Metric MutableCounterLong numVolumeCreates;
  private @Metric MutableCounterLong numVolumeUpdates;
  private @Metric MutableCounterLong numVolumeInfos;
  private @Metric MutableCounterLong numVolumeCheckAccesses;
  private @Metric MutableCounterLong numBucketCreates;
  private @Metric MutableCounterLong numFSOBucketCreates;
  private @Metric MutableCounterLong numVolumeDeletes;
  private @Metric MutableCounterLong numBucketInfos;
  private @Metric MutableCounterLong numBucketUpdates;
  private @Metric MutableCounterLong numBucketDeletes;
  private @Metric MutableCounterLong numFSOBucketDeletes;
  private @Metric MutableCounterLong numKeyAllocate;
  private @Metric MutableCounterLong numKeyLookup;
  private @Metric MutableCounterLong numKeyRenames;
  private @Metric MutableCounterLong numKeyDeletes;
  private @Metric MutableCounterLong numBucketLists;
  private @Metric MutableCounterLong numKeyLists;
  private @Metric MutableCounterLong numVolumeLists;
  private @Metric MutableCounterLong numKeyCommits;
  private @Metric MutableCounterLong numKeyHSyncs;
  private @Metric MutableCounterLong numBlockAllocations;
  private @Metric MutableCounterLong numGetServiceLists;
  private @Metric MutableCounterLong numBucketS3Lists;
  private @Metric MutableCounterLong numInitiateMultipartUploads;
  private @Metric MutableCounterLong numCompleteMultipartUploads;
  private @Metric MutableCounterLong numSnapshotCreates;
  private @Metric MutableCounterLong numSnapshotDeletes;
  private @Metric MutableCounterLong numSnapshotLists;
  private @Metric MutableCounterLong numSnapshotRenames;
  private @Metric MutableCounterLong numSnapshotDiffJobs;
  private @Metric MutableCounterLong numSnapshotInfos;
  private @Metric MutableCounterLong numCancelSnapshotDiffs;
  private @Metric MutableCounterLong numListSnapshotDiffJobs;

  private @Metric MutableGaugeInt numSnapshotCacheSize;
  private @Metric MutableCounterLong numGetFileStatus;
  private @Metric MutableCounterLong numCreateDirectory;
  private @Metric MutableCounterLong numCreateFile;
  private @Metric MutableCounterLong numLookupFile;
  private @Metric MutableCounterLong numListStatus;
  private @Metric MutableCounterLong numListOpenFiles;

  private @Metric MutableCounterLong numOpenKeyDeleteRequests;
  private @Metric MutableCounterLong numOpenKeysSubmittedForDeletion;
  private @Metric MutableCounterLong numOpenKeysDeleted;
  private @Metric MutableCounterLong numOpenKeysCleaned;
  private @Metric MutableCounterLong numOpenKeysHSyncCleaned;

  private @Metric MutableCounterLong numExpiredMPUAbortRequests;
  private @Metric MutableCounterLong numExpiredMPUSubmittedForAbort;
  private @Metric MutableCounterLong numExpiredMPUAborted;
  private @Metric MutableCounterLong numExpiredMPUPartsAborted;

  private @Metric MutableCounterLong numAddAcl;
  private @Metric MutableCounterLong numSetAcl;
  private @Metric MutableCounterLong numGetAcl;
  private @Metric MutableCounterLong numRemoveAcl;
  private @Metric MutableCounterLong numSetTime;
  private @Metric MutableCounterLong numGetKeyInfo;

  private @Metric MutableCounterLong numGetObjectTagging;
  private @Metric MutableCounterLong numPutObjectTagging;
  private @Metric MutableCounterLong numDeleteObjectTagging;

  private @Metric MutableCounterLong numLinearizableRead;
  private @Metric MutableCounterLong numLeaderSkipLinearizableRead;

  private @Metric MutableCounterLong numFollowerReadLocalLeaseSuccess;
  private @Metric MutableCounterLong numFollowerReadLocalLeaseFailLog;
  private @Metric MutableCounterLong numFollowerReadLocalLeaseFailTime;

  // Failure Metrics
  private @Metric MutableCounterLong numVolumeCreateFails;
  private @Metric MutableCounterLong numVolumeUpdateFails;
  private @Metric MutableCounterLong numVolumeInfoFails;
  private @Metric MutableCounterLong numVolumeDeleteFails;
  private @Metric MutableCounterLong numBucketCreateFails;
  private @Metric MutableCounterLong numVolumeCheckAccessFails;
  private @Metric MutableCounterLong numBucketInfoFails;
  private @Metric MutableCounterLong numBucketUpdateFails;
  private @Metric MutableCounterLong numBucketDeleteFails;
  private @Metric MutableCounterLong numKeyAllocateFails;
  private @Metric MutableCounterLong numKeyLookupFails;
  private @Metric MutableCounterLong numKeyRenameFails;
  private @Metric MutableCounterLong numKeyDeleteFails;
  private @Metric MutableCounterLong numBucketListFails;
  private @Metric MutableCounterLong numKeyListFails;
  private @Metric MutableCounterLong numVolumeListFails;
  private @Metric MutableCounterLong numKeyCommitFails;
  private @Metric MutableCounterLong numBlockAllocationFails;
  private @Metric MutableCounterLong numGetServiceListFails;
  private @Metric MutableCounterLong numBucketS3ListFails;
  private @Metric MutableCounterLong numInitiateMultipartUploadFails;
  private @Metric MutableCounterLong numCommitMultipartUploadParts;
  private @Metric MutableCounterLong numCommitMultipartUploadPartFails;
  private @Metric MutableCounterLong numCompleteMultipartUploadFails;
  private @Metric MutableCounterLong numAbortMultipartUploads;
  private @Metric MutableCounterLong numAbortMultipartUploadFails;
  private @Metric MutableCounterLong numListMultipartUploadParts;
  private @Metric MutableCounterLong numListMultipartUploadPartFails;
  private @Metric MutableCounterLong numOpenKeyDeleteRequestFails;
  private @Metric MutableCounterLong numExpiredMPUAbortRequestFails;
  private @Metric MutableCounterLong numSnapshotCreateFails;
  private @Metric MutableCounterLong numSnapshotRenameFails;
  private @Metric MutableCounterLong numSnapshotDeleteFails;
  private @Metric MutableCounterLong numSnapshotListFails;
  private @Metric MutableCounterLong numSnapshotDiffJobFails;
  private @Metric MutableCounterLong numSnapshotInfoFails;
  private @Metric MutableCounterLong numCancelSnapshotDiffFails;
  private @Metric MutableCounterLong numListSnapshotDiffJobFails;

  private @Metric MutableCounterLong numSnapshotActive;
  private @Metric MutableCounterLong numSnapshotDeleted;

  // Number of tenant operations attempted
  private @Metric MutableCounterLong numTenantOps;
  // Metrics for a total number of tenants
  private @Metric MutableCounterLong numTenants;
  // Metrics for tenant create operation
  private @Metric MutableCounterLong numTenantCreates;
  private @Metric MutableCounterLong numTenantCreateFails;
  // Metrics for tenant delete operation
  private @Metric MutableCounterLong numTenantDeletes;
  private @Metric MutableCounterLong numTenantDeleteFails;
  // Metrics for tenant assign user operation
  private @Metric MutableCounterLong numTenantAssignUsers;
  private @Metric MutableCounterLong numTenantAssignUserFails;
  // Metrics for tenant revoke user operation
  private @Metric MutableCounterLong numTenantRevokeUsers;
  private @Metric MutableCounterLong numTenantRevokeUserFails;
  // Metrics for tenant assign admin operation
  private @Metric MutableCounterLong numTenantAssignAdmins;
  private @Metric MutableCounterLong numTenantAssignAdminFails;
  // Metrics for tenant revoke admin operation
  private @Metric MutableCounterLong numTenantRevokeAdmins;
  private @Metric MutableCounterLong numTenantRevokeAdminFails;
  // Metric for tenant list operation
  private @Metric MutableCounterLong numTenantLists;
  // Metric for tenant get user info
  private @Metric MutableCounterLong numTenantGetUserInfos;
  // Metric for list users in tenant operation
  private @Metric MutableCounterLong numTenantTenantUserLists;

  private @Metric MutableCounterLong numRecoverLease;

  private @Metric MutableCounterLong numGetFileStatusFails;
  private @Metric MutableCounterLong numCreateDirectoryFails;
  private @Metric MutableCounterLong numCreateFileFails;
  private @Metric MutableCounterLong numLookupFileFails;
  private @Metric MutableCounterLong numListStatusFails;
  private @Metric MutableCounterLong numListOpenFilesFails;
  private @Metric MutableCounterLong getNumGetKeyInfoFails;

  private @Metric MutableCounterLong numGetObjectTaggingFails;
  private @Metric MutableCounterLong numPutObjectTaggingFails;
  private @Metric MutableCounterLong numDeleteObjectTaggingFails;

  private @Metric MutableCounterLong numRecoverLeaseFails;

  // Metrics for total amount of data written
  private @Metric MutableCounterLong totalDataCommitted;

  // Metrics for total number of volumes, buckets and keys

  private @Metric MutableCounterLong numVolumes;
  private @Metric MutableCounterLong numBuckets;
  private @Metric MutableCounterLong numS3Buckets;

  //TODO: This metric is an estimate and it may be inaccurate on restart if the
  // OM process was not shutdown cleanly. Key creations/deletions in the last
  // few minutes before restart may not be included in this count.
  private @Metric MutableCounterLong numKeys;

  private @Metric MutableCounterLong numBucketS3Creates;
  private @Metric MutableCounterLong numBucketS3CreateFails;
  private @Metric MutableCounterLong numBucketS3Deletes;
  private @Metric MutableCounterLong numBucketS3DeleteFails;

  private @Metric MutableCounterLong numListMultipartUploadFails;
  private @Metric MutableCounterLong numListMultipartUploads;

  // Metrics related to OM Trash.
  private @Metric MutableCounterLong numTrashRenames;
  private @Metric MutableCounterLong numTrashDeletes;
  private @Metric MutableCounterLong numTrashListStatus;
  private @Metric MutableCounterLong numTrashGetFileStatus;
  private @Metric MutableCounterLong numTrashGetTrashRoots;
  private @Metric MutableCounterLong numTrashExists;
  private @Metric MutableCounterLong numTrashWriteRequests;
  private @Metric MutableCounterLong numTrashFilesRenames;
  private @Metric MutableCounterLong numTrashFilesDeletes;
  private @Metric MutableCounterLong numTrashActiveCycles;
  private @Metric MutableCounterLong numTrashCheckpointsProcessed;
  private @Metric MutableCounterLong numTrashFails;
  private @Metric MutableCounterLong numTrashRootsEnqueued;
  private @Metric MutableCounterLong numTrashRootsProcessed;
  private @Metric MutableCounterLong numTrashAtomicDirRenames;
  private @Metric MutableCounterLong numTrashAtomicDirDeletes;

  //FSO Metrics
  private @Metric MutableCounterLong numDirs;
  private @Metric MutableCounterLong numFiles;

  //EC Metrics
  private @Metric MutableCounterLong ecKeyCreateTotal;
  private @Metric MutableCounterLong ecKeyCreateFailsTotal;
  private @Metric MutableCounterLong ecBucketCreateTotal;
  private @Metric MutableCounterLong ecBucketCreateFailsTotal;
  private final DBCheckpointMetrics dbCheckpointMetrics;

  public OMMetrics() {
    dbCheckpointMetrics = DBCheckpointMetrics.create("OM Metrics");
  }

  public static OMMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        "Ozone Manager Metrics",
        new OMMetrics());
  }

  public DBCheckpointMetrics getDBCheckpointMetrics() {
    return dbCheckpointMetrics;
  }

  public void incNumS3BucketCreates() {
    numBucketOps.incr();
    numBucketS3Creates.incr();
  }

  public void incNumS3BucketCreateFails() {
    numBucketS3CreateFails.incr();
  }

  public void incNumS3BucketDeletes() {
    numBucketOps.incr();
    numBucketS3Deletes.incr();
  }

  public void incNumS3BucketDeleteFails() {
    numBucketOps.incr();
    numBucketS3DeleteFails.incr();
  }

  public void incDataCommittedBytes(long bytesWritten) {
    totalDataCommitted.incr(bytesWritten);
  }

  public void incNumS3Buckets() {
    numS3Buckets.incr();
  }

  public void decNumS3Buckets() {
    numS3Buckets.incr();
  }

  public void incNumVolumes() {
    numVolumes.incr();
  }

  public void decNumVolumes() {
    numVolumes.incr(-1);
  }

  public void incNumBuckets() {
    numBuckets.incr();
  }

  public void decNumBuckets() {
    numBuckets.incr(-1);
  }

  public void incNumKeys() {
    numKeys.incr();
  }

  public void incNumKeys(int count) {
    numKeys.incr(count);
  }

  public void decNumKeys() {
    numKeys.incr(-1);
  }

  public void setNumVolumes(long val) {
    long oldVal = this.numVolumes.value();
    this.numVolumes.incr(val - oldVal);
  }

  public void setNumBuckets(long val) {
    long oldVal = this.numBuckets.value();
    this.numBuckets.incr(val - oldVal);
  }

  public void setNumKeys(long val) {
    long oldVal = this.numKeys.value();
    this.numKeys.incr(val - oldVal);
  }

  public void setNumDirs(long val) {
    long oldVal = this.numDirs.value();
    this.numDirs.incr(val - oldVal);
  }

  public void setNumFiles(long val) {
    long oldVal = this.numDirs.value();
    this.numDirs.incr(val - oldVal);
  }

  public void decNumKeys(long val) {
    this.numKeys.incr(-val);
  }

  public long getNumVolumes() {
    return numVolumes.value();
  }

  public long getNumBuckets() {
    return numBuckets.value();
  }

  public long getNumKeys() {
    return numKeys.value();
  }

  public void incNumVolumeCreates() {
    numVolumeOps.incr();
    numVolumeCreates.incr();
  }

  public void incNumVolumeUpdates() {
    numVolumeOps.incr();
    numVolumeUpdates.incr();
  }

  public void incNumVolumeInfos() {
    numVolumeOps.incr();
    numVolumeInfos.incr();
  }

  public void incNumVolumeDeletes() {
    numVolumeOps.incr();
    numVolumeDeletes.incr();
  }

  public void incNumVolumeCheckAccesses() {
    numVolumeOps.incr();
    numVolumeCheckAccesses.incr();
  }

  public void incNumBucketCreates() {
    numBucketOps.incr();
    numBucketCreates.incr();
  }

  public void incNumFSOBucketCreates() {
    numFSOBucketCreates.incr();
  }

  public void incNumBucketInfos() {
    numBucketOps.incr();
    numBucketInfos.incr();
  }

  public void incNumBucketUpdates() {
    numBucketOps.incr();
    numBucketUpdates.incr();
  }

  public void incNumBucketDeletes() {
    numBucketOps.incr();
    numBucketDeletes.incr();
  }

  public void incNumFSOBucketDeletes() {
    numFSOBucketDeletes.incr();
  }

  public void incNumBucketLists() {
    numBucketOps.incr();
    numBucketLists.incr();
  }

  @Override
  public void incNumKeyLists() {
    numKeyOps.incr();
    numKeyLists.incr();
  }

  public void incNumVolumeLists() {
    numVolumeOps.incr();
    numVolumeLists.incr();
  }

  public void incNumListS3Buckets() {
    numBucketOps.incr();
    numBucketS3Lists.incr();
  }

  public void incNumListS3BucketsFails() {
    numBucketS3ListFails.incr();
  }

  public void incNumListOpenFiles() {
    numKeyOps.incr();
    numListOpenFiles.incr();
  }

  public void incNumListOpenFilesFails() {
    numListOpenFilesFails.incr();
  }

  public void incNumInitiateMultipartUploads() {
    numKeyOps.incr();
    numInitiateMultipartUploads.incr();
  }

  public void incNumInitiateMultipartUploadFails() {
    numInitiateMultipartUploadFails.incr();
  }

  public void incNumCommitMultipartUploadParts() {
    numKeyOps.incr();
    numCommitMultipartUploadParts.incr();
  }

  public void incNumCommitMultipartUploadPartFails() {
    numCommitMultipartUploadPartFails.incr();
  }

  public void incNumCompleteMultipartUploads() {
    numKeyOps.incr();
    numCompleteMultipartUploads.incr();
  }

  public void incNumSnapshotCreates() {
    numSnapshotCreates.incr();
  }

  public void incNumSnapshotCreateFails() {
    numSnapshotCreateFails.incr();
  }

  public void incNumSnapshotRenames() {
    numSnapshotRenames.incr();
  }

  public void incNumSnapshotRenameFails() {
    numSnapshotRenameFails.incr();
  }

  public void incNumSnapshotDeletes() {
    numSnapshotDeletes.incr();
  }

  public void incNumSnapshotDeleteFails() {
    numSnapshotDeleteFails.incr();
  }

  public void incNumSnapshotLists() {
    numSnapshotLists.incr();
  }

  public void incNumSnapshotInfos() {
    numSnapshotInfos.incr();
  }

  public void incNumSnapshotDiffJobs() {
    numSnapshotDiffJobs.incr();
  }

  public void incNumCancelSnapshotDiffs() {
    numCancelSnapshotDiffs.incr();
  }

  public void incNumCancelSnapshotDiffJobFails() {
    numCancelSnapshotDiffFails.incr();
  }

  public void incNumListSnapshotDiffJobs() {
    numListSnapshotDiffJobs.incr();
  }

  public void incNumListSnapshotDiffJobFails() {
    numListSnapshotDiffJobFails.incr();
  }

  public void incNumSnapshotListFails() {
    numSnapshotListFails.incr();
  }

  public void incNumSnapshotDiffJobFails() {
    numSnapshotDiffJobFails.incr();
  }

  public void incNumSnapshotInfoFails() {
    numSnapshotInfoFails.incr();
  }

  public void setNumSnapshotActive(long num) {
    long currVal = numSnapshotActive.value();
    numSnapshotActive.incr(num - currVal);
  }

  public void incNumSnapshotActive() {
    numSnapshotActive.incr();
  }

  public void decNumSnapshotActive() {
    numSnapshotActive.incr(-1);
  }

  public void setNumSnapshotDeleted(long num) {
    long currVal = numSnapshotDeleted.value();
    numSnapshotDeleted.incr(num - currVal);
  }

  public void incNumSnapshotDeleted() {
    numSnapshotDeleted.incr();
  }

  public void decNumSnapshotDeleted() {
    numSnapshotDeleted.incr(-1);
  }

  public int getNumSnapshotCacheSize() {
    return numSnapshotCacheSize.value();
  }

  public void incNumSnapshotCacheSize() {
    numSnapshotCacheSize.incr();
  }

  public void decNumSnapshotCacheSize() {
    numSnapshotCacheSize.decr();
  }

  public void incNumCompleteMultipartUploadFails() {
    numCompleteMultipartUploadFails.incr();
  }

  public void incNumAbortMultipartUploads() {
    numKeyOps.incr();
    numAbortMultipartUploads.incr();
  }

  public void incNumListMultipartUploadFails() {
    numListMultipartUploadFails.incr();
  }

  public void incNumListMultipartUploads() {
    numKeyOps.incr();
    numListMultipartUploads.incr();
  }

  public void incNumAbortMultipartUploadFails() {
    numAbortMultipartUploadFails.incr();
  }

  public void incNumListMultipartUploadParts() {
    numKeyOps.incr();
    numListMultipartUploadParts.incr();
  }

  public void incNumTenants() {
    numTenants.incr();
  }

  public void decNumTenants() {
    numTenants.incr(-1);
  }

  public void incNumTenantCreates() {
    numTenantOps.incr();
    numTenantCreates.incr();
  }

  public void incNumTenantCreateFails() {
    numTenantCreateFails.incr();
  }

  public void incNumTenantDeletes() {
    numTenantOps.incr();
    numTenantDeletes.incr();
  }

  public void incNumTenantDeleteFails() {
    numTenantDeleteFails.incr();
  }

  public void incNumTenantAssignUsers() {
    numTenantOps.incr();
    numTenantAssignUsers.incr();
  }

  public void incNumTenantAssignUserFails() {
    numTenantAssignUserFails.incr();
  }

  public void incNumTenantRevokeUsers() {
    numTenantOps.incr();
    numTenantRevokeUsers.incr();
  }

  public void incNumTenantRevokeUserFails() {
    numTenantRevokeUserFails.incr();
  }

  public void incNumTenantAssignAdmins() {
    numTenantOps.incr();
    numTenantAssignAdmins.incr();
  }

  public void incNumTenantAssignAdminFails() {
    numTenantAssignAdminFails.incr();
  }

  public void incNumTenantRevokeAdmins() {
    numTenantOps.incr();
    numTenantRevokeAdmins.incr();
  }

  public void incNumTenantRevokeAdminFails() {
    numTenantRevokeAdminFails.incr();
  }

  public void incNumTenantLists() {
    numTenantOps.incr();
    numTenantLists.incr();
  }

  public void incNumTenantGetUserInfos() {
    numTenantOps.incr();
    numTenantGetUserInfos.incr();
  }

  public void incNumTenantUserLists() {
    numTenantOps.incr();
    numTenantTenantUserLists.incr();
  }

  @Override
  public void incNumGetFileStatus() {
    numKeyOps.incr();
    numFSOps.incr();
    numGetFileStatus.incr();
  }

  @Override
  public void incNumGetFileStatusFails() {
    numGetFileStatusFails.incr();
  }

  public void incNumCreateDirectory() {
    numKeyOps.incr();
    numFSOps.incr();
    numCreateDirectory.incr();
  }

  public void incNumCreateDirectoryFails() {
    numCreateDirectoryFails.incr();
  }

  public void incNumCreateFile() {
    numKeyOps.incr();
    numFSOps.incr();
    numCreateFile.incr();
  }

  public void incNumCreateFileFails() {
    numCreateFileFails.incr();
  }

  @Override
  public void incNumLookupFile() {
    numKeyOps.incr();
    numFSOps.incr();
    numLookupFile.incr();
  }

  @Override
  public void incNumLookupFileFails() {
    numLookupFileFails.incr();
  }

  @Override
  public void incNumListStatus() {
    numKeyOps.incr();
    numFSOps.incr();
    numListStatus.incr();
  }

  @Override
  public void incNumListStatusFails() {
    numListStatusFails.incr();
  }

  public void incNumListMultipartUploadPartFails() {
    numListMultipartUploadPartFails.incr();
  }

  public void incNumGetServiceLists() {
    numGetServiceLists.incr();
  }

  public void incNumVolumeCreateFails() {
    numVolumeCreateFails.incr();
  }

  public void incNumVolumeUpdateFails() {
    numVolumeUpdateFails.incr();
  }

  public void incNumVolumeInfoFails() {
    numVolumeInfoFails.incr();
  }

  public void incNumVolumeDeleteFails() {
    numVolumeDeleteFails.incr();
  }

  public void incNumVolumeCheckAccessFails() {
    numVolumeCheckAccessFails.incr();
  }

  public void incNumBucketCreateFails() {
    numBucketCreateFails.incr();
  }

  public void incNumBucketInfoFails() {
    numBucketInfoFails.incr();
  }

  public void incNumBucketUpdateFails() {
    numBucketUpdateFails.incr();
  }

  public void incNumBucketDeleteFails() {
    numBucketDeleteFails.incr();
  }

  public void incNumKeyAllocates() {
    numKeyOps.incr();
    numKeyAllocate.incr();
  }

  public void incNumKeyAllocateFails() {
    numKeyAllocateFails.incr();
  }

  @Override
  public void incNumKeyLookups() {
    numKeyOps.incr();
    numKeyLookup.incr();
  }

  @Override
  public void incNumKeyLookupFails() {
    numKeyLookupFails.incr();
  }

  public void incNumKeyRenames() {
    numKeyOps.incr();
    numKeyRenames.incr();
  }

  public void incNumKeyRenameFails() {
    numKeyRenameFails.incr();
  }

  public void incNumKeyDeleteFails() {
    numKeyDeleteFails.incr();
  }

  public void incNumKeyDeletes() {
    numKeyOps.incr();
    numKeyDeletes.incr();
  }

  public void incNumKeyCommits() {
    numKeyOps.incr();
    numKeyCommits.incr();
  }

  public void incNumKeyHSyncs() {
    numKeyOps.incr();
    numKeyHSyncs.incr();
  }

  public void incNumKeyCommitFails() {
    numKeyCommitFails.incr();
  }

  public void incNumBlockAllocateCalls() {
    numBlockAllocations.incr();
  }

  public void incNumBlockAllocateCallFails() {
    numBlockAllocationFails.incr();
  }

  public void incNumBucketListFails() {
    numBucketListFails.incr();
  }

  @Override
  public void incNumKeyListFails() {
    numKeyListFails.incr();
  }

  public void incNumVolumeListFails() {
    numVolumeListFails.incr();
  }

  public void incNumGetServiceListFails() {
    numGetServiceListFails.incr();
  }

  public void incNumOpenKeyDeleteRequests() {
    numOpenKeyDeleteRequests.incr();
  }

  public void incNumOpenKeysSubmittedForDeletion(long amount) {
    numOpenKeysSubmittedForDeletion.incr(amount);
  }

  public void incNumOpenKeysDeleted() {
    numOpenKeysDeleted.incr();
  }

  public void incNumOpenKeysCleaned(int delta) {
    numOpenKeysCleaned.incr(delta);
  }

  public void incNumOpenKeysHSyncCleaned() {
    numOpenKeysHSyncCleaned.incr();
  }

  public void incNumOpenKeyDeleteRequestFails() {
    numOpenKeyDeleteRequestFails.incr();
  }

  public void incNumExpiredMPUAbortRequests() {
    numExpiredMPUAbortRequests.incr();
  }

  public void incNumExpiredMPUSubmittedForAbort(long amount) {
    numExpiredMPUSubmittedForAbort.incr(amount);
  }

  public void incNumExpiredMPUAborted() {
    numExpiredMPUAborted.incr();
  }

  public void incNumExpiredMPUPartsAborted(long amount) {
    numExpiredMPUPartsAborted.incr(amount);
  }

  public void incNumExpiredMpuAbortRequestFails() {
    numExpiredMPUAbortRequestFails.incr();
  }

  public void incNumAddAcl() {
    numAddAcl.incr();
  }

  public void incNumSetAcl() {
    numSetAcl.incr();
  }

  @Override
  public void incNumGetAcl() {
    numGetAcl.incr();
  }

  public void incNumRemoveAcl() {
    numRemoveAcl.incr();
  }

  public void incNumSetTime() {
    numSetTime.incr();
  }

  @Override
  public void incNumGetKeyInfo() {
    numGetKeyInfo.incr();
    numKeyOps.incr();
  }

  @Override
  public void incNumGetKeyInfoFails() {
    getNumGetKeyInfoFails.incr();
  }

  @Override
  public void incNumGetObjectTagging() {
    numGetObjectTagging.incr();
    numKeyOps.incr();
  }

  @Override
  public void incNumGetObjectTaggingFails() {
    numGetObjectTaggingFails.incr();
  }

  public void incNumPutObjectTagging() {
    numPutObjectTagging.incr();
    numKeyOps.incr();
  }

  public void incNumPutObjectTaggingFails() {
    numPutObjectTaggingFails.incr();
  }

  public void incNumDeleteObjectTagging() {
    numDeleteObjectTagging.incr();
    numKeyOps.incr();
  }

  public void incNumDeleteObjectTaggingFails() {
    numDeleteObjectTaggingFails.incr();
  }

  public void incNumLinearizableRead() {
    numLinearizableRead.incr();
  }

  public long getNumLinearizableRead() {
    return numLinearizableRead.value();
  }

  public void incNumLeaderSkipLinearizableRead() {
    numLeaderSkipLinearizableRead.incr();
  }

  public long getNumLeaderSkipLinearizableRead() {
    return numLeaderSkipLinearizableRead.value();
  }

  public void incNumFollowerReadLocalLeaseSuccess() {
    numFollowerReadLocalLeaseSuccess.incr();
  }

  public long getNumFollowerReadLocalLeaseSuccess() {
    return numFollowerReadLocalLeaseSuccess.value();
  }

  public void incNumFollowerReadLocalLeaseFailLog() {
    numFollowerReadLocalLeaseFailLog.incr();
  }

  public long getNumFollowerReadLocalLeaseFailLog() {
    return numFollowerReadLocalLeaseFailLog.value();
  }

  public void incNumFollowerReadLocalLeaseFailTime() {
    numFollowerReadLocalLeaseFailTime.incr();
  }

  public long getNumFollowerReadLocalLeaseFailTime() {
    return numFollowerReadLocalLeaseFailTime.value();
  }

  @VisibleForTesting
  public long getNumVolumeCreates() {
    return numVolumeCreates.value();
  }

  @VisibleForTesting
  public long getNumVolumeUpdates() {
    return numVolumeUpdates.value();
  }

  @VisibleForTesting
  public long getNumVolumeInfos() {
    return numVolumeInfos.value();
  }

  @VisibleForTesting
  public long getNumVolumeDeletes() {
    return numVolumeDeletes.value();
  }

  @VisibleForTesting
  public long getNumVolumeCheckAccesses() {
    return numVolumeCheckAccesses.value();
  }

  @VisibleForTesting
  public long getNumBucketCreates() {
    return numBucketCreates.value();
  }

  @VisibleForTesting
  public long getNumFSOBucketCreates() {
    return numFSOBucketCreates.value();
  }

  @VisibleForTesting
  public long getNumBucketInfos() {
    return numBucketInfos.value();
  }

  @VisibleForTesting
  public long getNumBucketUpdates() {
    return numBucketUpdates.value();
  }

  @VisibleForTesting
  public long getNumBucketDeletes() {
    return numBucketDeletes.value();
  }

  @VisibleForTesting
  public long getNumFSOBucketDeletes() {
    return numFSOBucketDeletes.value();
  }

  @VisibleForTesting
  public long getNumBucketLists() {
    return numBucketLists.value();
  }

  @VisibleForTesting
  public long getNumVolumeLists() {
    return numVolumeLists.value();
  }

  @VisibleForTesting
  public long getNumKeyLists() {
    return numKeyLists.value();
  }

  @VisibleForTesting
  public long getNumGetServiceLists() {
    return numGetServiceLists.value();
  }

  @VisibleForTesting
  public long getNumVolumeCreateFails() {
    return numVolumeCreateFails.value();
  }

  @VisibleForTesting
  public long getNumVolumeUpdateFails() {
    return numVolumeUpdateFails.value();
  }

  @VisibleForTesting
  public long getNumVolumeInfoFails() {
    return numVolumeInfoFails.value();
  }

  @VisibleForTesting
  public long getNumVolumeDeleteFails() {
    return numVolumeDeleteFails.value();
  }

  @VisibleForTesting
  public long getNumVolumeCheckAccessFails() {
    return numVolumeCheckAccessFails.value();
  }

  @VisibleForTesting
  public long getNumBucketCreateFails() {
    return numBucketCreateFails.value();
  }

  @VisibleForTesting
  public long getNumBucketInfoFails() {
    return numBucketInfoFails.value();
  }

  @VisibleForTesting
  public long getNumBucketUpdateFails() {
    return numBucketUpdateFails.value();
  }

  @VisibleForTesting
  public long getNumBucketDeleteFails() {
    return numBucketDeleteFails.value();
  }

  @VisibleForTesting
  public long getNumKeyAllocates() {
    return numKeyAllocate.value();
  }

  @VisibleForTesting
  public long getNumKeyAllocateFails() {
    return numKeyAllocateFails.value();
  }

  @VisibleForTesting
  public long getNumKeyLookups() {
    return numKeyLookup.value();
  }

  @VisibleForTesting
  public long getNumKeyLookupFails() {
    return numKeyLookupFails.value();
  }

  @VisibleForTesting
  public long getNumKeyRenames() {
    return numKeyRenames.value();
  }

  @VisibleForTesting
  public long getNumKeyRenameFails() {
    return numKeyRenameFails.value();
  }

  @VisibleForTesting
  public long getNumKeyDeletes() {
    return numKeyDeletes.value();
  }

  @VisibleForTesting
  public long getNumKeyDeletesFails() {
    return numKeyDeleteFails.value();
  }

  @VisibleForTesting
  public long getNumBucketListFails() {
    return numBucketListFails.value();
  }

  @VisibleForTesting
  public long getNumKeyListFails() {
    return numKeyListFails.value();
  }

  @VisibleForTesting
  public long getNumFSOps() {
    return numFSOps.value();
  }

  @VisibleForTesting
  public long getNumGetFileStatus() {
    return numGetFileStatus.value();
  }

  @VisibleForTesting
  public long getNumListStatus() {
    return numListStatus.value();
  }

  @VisibleForTesting
  public long getNumVolumeListFails() {
    return numVolumeListFails.value();
  }

  @VisibleForTesting
  public long getNumKeyCommits() {
    return numKeyCommits.value();
  }

  @VisibleForTesting
  public long getNumKeyHSyncs() {
    return numKeyHSyncs.value();
  }

  @VisibleForTesting
  public void resetNumKeyHSyncs() {
    numKeyHSyncs.incr(-numKeyHSyncs.value());
  }

  @VisibleForTesting
  public long getNumKeyCommitFails() {
    return numKeyCommitFails.value();
  }

  @VisibleForTesting
  public long getNumBlockAllocates() {
    return numBlockAllocations.value();
  }

  @VisibleForTesting
  public long getNumBlockAllocateFails() {
    return numBlockAllocationFails.value();
  }

  @VisibleForTesting
  public long getNumGetServiceListFails() {
    return numGetServiceListFails.value();
  }

  @VisibleForTesting
  public long getNumListS3Buckets() {
    return numBucketS3Lists.value();
  }

  @VisibleForTesting
  public long getNumListS3BucketsFails() {
    return numBucketS3ListFails.value();
  }

  @VisibleForTesting
  public long getDataCommittedBytes() {
    return totalDataCommitted.value();
  }

  public long getNumInitiateMultipartUploads() {
    return numInitiateMultipartUploads.value();
  }

  public long getNumInitiateMultipartUploadFails() {
    return numInitiateMultipartUploadFails.value();
  }

  public long getNumAbortMultipartUploads() {
    return numAbortMultipartUploads.value();
  }

  public long getNumAbortMultipartUploadFails() {
    return numAbortMultipartUploadFails.value();
  }

  public long getNumOpenKeyDeleteRequests() {
    return numOpenKeyDeleteRequests.value();
  }

  public long getNumOpenKeysSubmittedForDeletion() {
    return numOpenKeysSubmittedForDeletion.value();
  }

  public long getNumOpenKeysDeleted() {
    return numOpenKeysDeleted.value();
  }

  public long getNumOpenKeysCleaned() {
    return numOpenKeysCleaned.value();
  }

  public long getNumOpenKeysHSyncCleaned() {
    return numOpenKeysHSyncCleaned.value();
  }

  public long getNumOpenKeyDeleteRequestFails() {
    return numOpenKeyDeleteRequestFails.value();
  }

  @VisibleForTesting
  public long getNumExpiredMPUAbortRequests() {
    return numExpiredMPUAbortRequests.value();
  }

  @VisibleForTesting
  public long getNumExpiredMPUSubmittedForAbort() {
    return numExpiredMPUSubmittedForAbort.value();
  }

  @VisibleForTesting
  public long getNumExpiredMPUAborted() {
    return numExpiredMPUAborted.value();
  }

  @VisibleForTesting
  public long getNumExpiredMPUAbortRequestFails() {
    return numExpiredMPUAbortRequestFails.value();
  }

  @VisibleForTesting
  public long getNumExpiredMPUPartsAborted() {
    return numExpiredMPUPartsAborted.value();
  }

  public long getNumAddAcl() {
    return numAddAcl.value();
  }

  public long getNumSetAcl() {
    return numSetAcl.value();
  }

  public long getNumGetAcl() {
    return numGetAcl.value();
  }

  public long getNumRemoveAcl() {
    return numRemoveAcl.value();
  }

  public long getNumTenantOps() {
    return numTenantOps.value();
  }

  public long getNumTenants() {
    return numTenants.value();
  }

  public long getNumTenantCreates() {
    return numTenantCreates.value();
  }

  public long getNumTenantCreateFails() {
    return numTenantCreateFails.value();
  }

  public long getNumTenantDeletes() {
    return numTenantDeletes.value();
  }

  public long getNumTenantDeleteFails() {
    return numTenantDeleteFails.value();
  }

  public long getNumTenantAssignUsers() {
    return numTenantAssignUsers.value();
  }

  public long getNumTenantAssignUserFails() {
    return numTenantAssignUserFails.value();
  }

  public long getNumTenantRevokeUsers() {
    return numTenantRevokeUsers.value();
  }

  public long getNumTenantRevokeUserFails() {
    return numTenantRevokeUserFails.value();
  }

  public long getNumTenantAssignAdmins() {
    return numTenantAssignAdmins.value();
  }

  public long getNumTenantAssignAdminFails() {
    return numTenantAssignAdminFails.value();
  }

  public long getNumTenantRevokeAdmins() {
    return numTenantRevokeAdmins.value();
  }

  public long getNumTenantRevokeAdminFails() {
    return numTenantRevokeAdminFails.value();
  }

  public long getNumTenantLists() {
    return numTenantLists.value();
  }

  public long getNumTenantGetUserInfos() {
    return numTenantGetUserInfos.value();
  }

  public long getNumTenantTenantUserLists() {
    return numTenantTenantUserLists.value();
  }

  public long getNumSnapshotCreates() {
    return numSnapshotCreates.value();
  }

  public long getNumSnapshotDeletes() {
    return numSnapshotDeletes.value();
  }

  public long getNumSnapshotLists() {
    return numSnapshotLists.value();
  }

  public long getNumSnapshotDiffJobs() {
    return numSnapshotDiffJobs.value();
  }

  public long getNumSnapshotCreateFails() {
    return numSnapshotCreateFails.value();
  }

  public long getNumSnapshotDeleteFails() {
    return numSnapshotDeleteFails.value();
  }

  public long getNumSnapshotListFails() {
    return numSnapshotListFails.value();
  }

  public long getNumSnapshotDiffJobFails() {
    return numSnapshotDiffJobFails.value();
  }

  public long getNumSnapshotActive() {
    return numSnapshotActive.value();
  }

  public long getNumSnapshotDeleted() {
    return numSnapshotDeleted.value();
  }

  public void incNumTrashRenames() {
    numTrashRenames.incr();
  }

  public long getNumTrashRenames() {
    return numTrashRenames.value();
  }

  public void incNumTrashDeletes() {
    numTrashDeletes.incr();
  }

  public long getNumTrashDeletes() {
    return numTrashDeletes.value();
  }

  public void incNumTrashListStatus() {
    numTrashListStatus.incr();
  }

  public void incNumTrashGetFileStatus() {
    numTrashGetFileStatus.incr();
  }

  public void incNumTrashGetTrashRoots() {
    numTrashGetTrashRoots.incr();
  }

  public void incNumTrashExists() {
    numTrashExists.incr();
  }

  public void incNumTrashWriteRequests() {
    numTrashWriteRequests.incr();
  }

  public void incNumTrashFilesRenames() {
    numTrashFilesRenames.incr();
  }

  public long getNumTrashFilesRenames() {
    return numTrashFilesRenames.value();
  }

  public void incNumTrashFilesDeletes() {
    numTrashFilesDeletes.incr();
  }

  public long getNumTrashFilesDeletes() {
    return numTrashFilesDeletes.value();
  }

  public long getNumTrashAtomicDirRenames() {
    return numTrashAtomicDirRenames.value();
  }

  public long getNumTrashAtomicDirDeletes() {
    return numTrashAtomicDirDeletes.value();
  }

  public void incNumTrashActiveCycles() {
    numTrashActiveCycles.incr();
  }

  public void incNumTrashRootsEnqueued() {
    numTrashRootsEnqueued.incr();
  }

  public void incNumTrashRootsProcessed() {
    numTrashRootsProcessed.incr();
  }

  public void incNumTrashFails() {
    numTrashFails.incr();
  }

  public void incNumTrashAtomicDirRenames() {
    numTrashAtomicDirRenames.incr();
  }

  public void incNumTrashAtomicDirDeletes() {
    numTrashAtomicDirDeletes.incr();
  }

  public void incEcKeysTotal() {
    ecKeyCreateTotal.incr();
  }

  public void incEcBucketsTotal() {
    ecBucketCreateTotal.incr();
  }

  public void incEcKeyCreateFailsTotal() {
    ecKeyCreateFailsTotal.incr();
  }

  public void incEcBucketCreateFailsTotal() {
    ecBucketCreateFailsTotal.incr();
  }

  public void incNumRecoverLease() {
    numKeyOps.incr();
    numFSOps.incr();
    numRecoverLease.incr();
  }

  public void incNumRecoverLeaseFails() {
    numRecoverLeaseFails.incr();
  }

  public void unRegister() {
    if (dbCheckpointMetrics != null) {
      dbCheckpointMetrics.unRegister();
    }
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }
}
