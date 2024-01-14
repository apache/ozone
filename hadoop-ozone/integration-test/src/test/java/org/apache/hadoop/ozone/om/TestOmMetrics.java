/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_ROOT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.apache.ozone.test.MetricsAsserts.assertCounter;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.apache.ozone.test.MetricsAsserts;
import org.apache.ozone.test.GenericTestUtils;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test for OM metrics.
 */
@Timeout(300)
public class TestOmMetrics {
  private MiniOzoneCluster cluster;
  private MiniOzoneCluster.Builder clusterBuilder;
  private OzoneConfiguration conf;
  private OzoneManager ozoneManager;
  private OzoneManagerProtocol writeClient;
  /**
   * The exception used for testing failure metrics.
   */
  private final OMException exception =
      new OMException("dummyException", OMException.ResultCodes.TIMEOUT);
  private OzoneClient client;

  /**
   * Create a MiniDFSCluster for testing.
   */
  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(OMConfigKeys.OZONE_OM_METRICS_SAVE_INTERVAL,
        1000, TimeUnit.MILLISECONDS);
    clusterBuilder = MiniOzoneCluster.newBuilder(conf).withoutDatanodes();
  }

  private void startCluster() throws Exception {
    cluster = clusterBuilder.build();
    cluster.waitForClusterToBeReady();
    ozoneManager = cluster.getOzoneManager();
    client = cluster.newClient();
    writeClient = client.getObjectStore()
        .getClientProxy().getOzoneManagerClient();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testVolumeOps() throws Exception {
    startCluster();
    VolumeManager volumeManager =
        (VolumeManager) HddsWhiteboxTestUtils.getInternalState(
            ozoneManager, "volumeManager");
    VolumeManager mockVm = spy(volumeManager);

    OmVolumeArgs volumeArgs = createVolumeArgs();
    doVolumeOps(volumeArgs);
    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    assertCounter("NumVolumeOps", 5L, omMetrics);
    assertCounter("NumVolumeCreates", 1L, omMetrics);
    assertCounter("NumVolumeUpdates", 1L, omMetrics);
    assertCounter("NumVolumeInfos", 1L, omMetrics);
    assertCounter("NumVolumeDeletes", 1L, omMetrics);
    assertCounter("NumVolumeLists", 1L, omMetrics);
    assertCounter("NumVolumes", 1L, omMetrics);

    volumeArgs = createVolumeArgs();
    writeClient.createVolume(volumeArgs);
    volumeArgs = createVolumeArgs();
    writeClient.createVolume(volumeArgs);
    volumeArgs = createVolumeArgs();
    writeClient.createVolume(volumeArgs);
    writeClient.deleteVolume(volumeArgs.getVolume());

    omMetrics = getMetrics("OMMetrics");

    // Accounting 's3v' volume which is created by default.
    assertCounter("NumVolumes", 3L, omMetrics);


    // inject exception to test for Failure Metrics on the read path
    doThrow(exception).when(mockVm).getVolumeInfo(any());
    doThrow(exception).when(mockVm).listVolumes(any(), any(),
        any(), anyInt());

    HddsWhiteboxTestUtils.setInternalState(ozoneManager,
        "volumeManager", mockVm);
    // inject exception to test for Failure Metrics on the write path
    mockWritePathExceptions(OmVolumeArgs.class);
    volumeArgs = createVolumeArgs();
    doVolumeOps(volumeArgs);

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumVolumeOps", 14L, omMetrics);
    assertCounter("NumVolumeCreates", 5L, omMetrics);
    assertCounter("NumVolumeUpdates", 2L, omMetrics);
    assertCounter("NumVolumeInfos", 2L, omMetrics);
    assertCounter("NumVolumeDeletes", 3L, omMetrics);
    assertCounter("NumVolumeLists", 2L, omMetrics);

    assertCounter("NumVolumeCreateFails", 1L, omMetrics);
    assertCounter("NumVolumeUpdateFails", 1L, omMetrics);
    assertCounter("NumVolumeInfoFails", 1L, omMetrics);
    assertCounter("NumVolumeDeleteFails", 1L, omMetrics);
    assertCounter("NumVolumeListFails", 1L, omMetrics);

    // As last call for volumesOps does not increment numVolumes as those are
    // failed.
    assertCounter("NumVolumes", 3L, omMetrics);

    cluster.restartOzoneManager();
    assertCounter("NumVolumes", 3L, omMetrics);


  }

  @Test
  public void testBucketOps() throws Exception {
    startCluster();
    BucketManager bucketManager =
        (BucketManager) HddsWhiteboxTestUtils.getInternalState(
            ozoneManager, "bucketManager");
    BucketManager mockBm = spy(bucketManager);

    OmBucketInfo bucketInfo = createBucketInfo(false);
    doBucketOps(bucketInfo);

    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    assertCounter("NumBucketOps", 5L, omMetrics);
    assertCounter("NumBucketCreates", 1L, omMetrics);
    assertCounter("NumBucketUpdates", 1L, omMetrics);
    assertCounter("NumBucketInfos", 1L, omMetrics);
    assertCounter("NumBucketDeletes", 1L, omMetrics);
    assertCounter("NumBucketLists", 1L, omMetrics);
    assertCounter("NumBuckets", 0L, omMetrics);

    OmBucketInfo ecBucketInfo = createBucketInfo(true);
    writeClient.createBucket(ecBucketInfo);
    writeClient.deleteBucket(ecBucketInfo.getVolumeName(),
        ecBucketInfo.getBucketName());
    omMetrics = getMetrics("OMMetrics");
    assertCounter("EcBucketCreateTotal", 1L, omMetrics);

    bucketInfo = createBucketInfo(false);
    writeClient.createBucket(bucketInfo);
    bucketInfo = createBucketInfo(false);
    writeClient.createBucket(bucketInfo);
    bucketInfo = createBucketInfo(false);
    writeClient.createBucket(bucketInfo);
    writeClient.deleteBucket(bucketInfo.getVolumeName(),
        bucketInfo.getBucketName());

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumBuckets", 2L, omMetrics);

    // inject exception to test for Failure Metrics on the read path
    doThrow(exception).when(mockBm).getBucketInfo(any(), any());
    doThrow(exception).when(mockBm).listBuckets(any(), any(),
        any(), anyInt(), eq(false));

    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "bucketManager", mockBm);

    // inject exception to test for Failure Metrics on the write path
    mockWritePathExceptions(OmBucketInfo.class);
    doBucketOps(bucketInfo);

    ecBucketInfo = createBucketInfo(true);
    try {
      writeClient.createBucket(ecBucketInfo);
    } catch (Exception e) {
      //Expected failure
    }
    omMetrics = getMetrics("OMMetrics");
    assertCounter("EcBucketCreateFailsTotal", 1L, omMetrics);

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumBucketOps", 17L, omMetrics);
    assertCounter("NumBucketCreates", 7L, omMetrics);
    assertCounter("NumBucketUpdates", 2L, omMetrics);
    assertCounter("NumBucketInfos", 2L, omMetrics);
    assertCounter("NumBucketDeletes", 4L, omMetrics);
    assertCounter("NumBucketLists", 2L, omMetrics);

    assertCounter("NumBucketCreateFails", 2L, omMetrics);
    assertCounter("NumBucketUpdateFails", 1L, omMetrics);
    assertCounter("NumBucketInfoFails", 1L, omMetrics);
    assertCounter("NumBucketDeleteFails", 1L, omMetrics);
    assertCounter("NumBucketListFails", 1L, omMetrics);

    assertCounter("NumBuckets", 2L, omMetrics);

    cluster.restartOzoneManager();
    assertCounter("NumBuckets", 2L, omMetrics);
  }

  @Test
  public void testKeyOps() throws Exception {
    // This test needs a cluster with DNs and SCM to wait on safemode
    clusterBuilder.setNumDatanodes(5);
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED, true);
    startCluster();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    KeyManager keyManager = (KeyManager) HddsWhiteboxTestUtils
        .getInternalState(ozoneManager, "keyManager");
    KeyManager mockKm = spy(keyManager);
    // see HDDS-10078 for making this work with FILE_SYSTEM_OPTIMIZED layout
    TestDataUtil.createVolumeAndBucket(client, volumeName, bucketName, BucketLayout.LEGACY);
    OmKeyArgs keyArgs = createKeyArgs(volumeName, bucketName,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
    doKeyOps(keyArgs);

    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeyOps", 7L, omMetrics);
    assertCounter("NumKeyAllocate", 1L, omMetrics);
    assertCounter("NumKeyLookup", 1L, omMetrics);
    assertCounter("NumKeyDeletes", 1L, omMetrics);
    assertCounter("NumKeyLists", 1L, omMetrics);
    assertCounter("NumTrashKeyLists", 1L, omMetrics);
    assertCounter("NumKeys", 0L, omMetrics);
    assertCounter("NumInitiateMultipartUploads", 1L, omMetrics);

    keyArgs = createKeyArgs(volumeName, bucketName,
        new ECReplicationConfig("rs-3-2-1024K"));
    doKeyOps(keyArgs);
    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeyOps", 14L, omMetrics);
    assertCounter("EcKeyCreateTotal", 1L, omMetrics);

    keyArgs = createKeyArgs(volumeName, bucketName,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
    OpenKeySession keySession = writeClient.openKey(keyArgs);
    writeClient.commitKey(keyArgs, keySession.getId());
    keyArgs = createKeyArgs(volumeName, bucketName,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
    keySession = writeClient.openKey(keyArgs);
    writeClient.commitKey(keyArgs, keySession.getId());
    keyArgs = createKeyArgs(volumeName, bucketName,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
    keySession = writeClient.openKey(keyArgs);
    writeClient.commitKey(keyArgs, keySession.getId());
    writeClient.deleteKey(keyArgs);

    keyArgs = createKeyArgs(volumeName, bucketName,
        new ECReplicationConfig("rs-6-3-1024K"));
    try {
      keySession = writeClient.openKey(keyArgs);
      writeClient.commitKey(keyArgs, keySession.getId());
    } catch (Exception e) {
      //Expected Failure in preExecute due to not enough datanode
      assertThat(e.getMessage()).contains("No enough datanodes to choose");
    }

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeys", 2L, omMetrics);
    assertCounter("NumBlockAllocationFails", 1L, omMetrics);

    // inject exception to test for Failure Metrics on the read path
    doThrow(exception).when(mockKm).lookupKey(any(), any(), any());
    doThrow(exception).when(mockKm).listKeys(
        any(), any(), any(), any(), anyInt());
    doThrow(exception).when(mockKm).listTrash(
        any(), any(), any(), any(), anyInt());
    OmMetadataReader omMetadataReader =
        (OmMetadataReader) ozoneManager.getOmMetadataReader().get();
    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "keyManager", mockKm);

    HddsWhiteboxTestUtils.setInternalState(
        omMetadataReader, "keyManager", mockKm);

    // inject exception to test for Failure Metrics on the write path
    mockWritePathExceptions(OmBucketInfo.class);
    keyArgs = createKeyArgs(volumeName, bucketName,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
    doKeyOps(keyArgs);

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeyOps", 28L, omMetrics);
    assertCounter("NumKeyAllocate", 6L, omMetrics);
    assertCounter("NumKeyLookup", 3L, omMetrics);
    assertCounter("NumKeyDeletes", 4L, omMetrics);
    assertCounter("NumKeyLists", 3L, omMetrics);
    assertCounter("NumTrashKeyLists", 3L, omMetrics);
    assertCounter("NumInitiateMultipartUploads", 3L, omMetrics);

    assertCounter("NumKeyAllocateFails", 1L, omMetrics);
    assertCounter("NumKeyLookupFails", 1L, omMetrics);
    assertCounter("NumKeyDeleteFails", 1L, omMetrics);
    assertCounter("NumKeyListFails", 1L, omMetrics);
    assertCounter("NumTrashKeyListFails", 1L, omMetrics);
    assertCounter("NumInitiateMultipartUploadFails", 1L, omMetrics);
    assertCounter("NumKeys", 2L, omMetrics);

    keyArgs = createKeyArgs(volumeName, bucketName,
        new ECReplicationConfig("rs-3-2-1024K"));
    try {
      keySession = writeClient.openKey(keyArgs);
      writeClient.commitKey(keyArgs, keySession.getId());
    } catch (Exception e) {
      //Expected Failure
    }
    omMetrics = getMetrics("OMMetrics");
    assertCounter("EcKeyCreateFailsTotal", 1L, omMetrics);

    cluster.restartOzoneManager();
    assertCounter("NumKeys", 2L, omMetrics);

  }

  @ParameterizedTest
  @EnumSource(value = BucketLayout.class, names = {"FILE_SYSTEM_OPTIMIZED", "LEGACY"})
  public void testDirectoryOps(BucketLayout bucketLayout) throws Exception {
    clusterBuilder.setNumDatanodes(3);
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED, true);
    // Speed up background directory deletion for this test.
    conf.setTimeDuration(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 1, TimeUnit.SECONDS);
    conf.set(OzoneConfigKeys.OZONE_CLIENT_FS_DEFAULT_BUCKET_LAYOUT, bucketLayout.name());
    // For testing fs operations with legacy buckets.
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    startCluster();

    // How long to wait for directory deleting service to clean up the files before aborting the test.
    final int timeoutMillis =
        (int)conf.getTimeDuration(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 0, TimeUnit.MILLISECONDS) * 3;
    assertTrue(timeoutMillis > 0, "Failed to read directory deleting service interval. Retrieved " + timeoutMillis +
        " milliseconds");

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    // Cluster should be empty.
    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeys", 0L, omMetrics);
    assertCounter("NumCreateDirectory", 0L, omMetrics);
    // These key operations include directory operations.
    assertCounter("NumKeyDeletes", 0L, omMetrics);
    assertCounter("NumKeyRenames", 0L, omMetrics);

    // Create bucket with 2 nested directories.
    String rootPath = String.format("%s://%s/",
        OzoneConsts.OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    FileSystem fs = FileSystem.get(conf);
    Path bucketPath = new Path(OZONE_ROOT, String.join(OZONE_URI_DELIMITER, volumeName, bucketName));
    Path dirPath = new Path(bucketPath, String.join(OZONE_URI_DELIMITER, "dir1", "dir2"));
    fs.mkdirs(dirPath);
    assertEquals(bucketLayout,
        client.getObjectStore().getVolume(volumeName).getBucket(bucketName).getBucketLayout());
    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeys", 2L, omMetrics);
    // Only one directory create command is given, even though it created two directories.
    assertCounter("NumCreateDirectory", 1L, omMetrics);
    assertCounter("NumKeyDeletes", 0L, omMetrics);
    assertCounter("NumKeyRenames", 0L, omMetrics);

    // Add 2 files at different parts of the tree.
    ContractTestUtils.touch(fs, new Path(dirPath, "file1"));
    ContractTestUtils.touch(fs, new Path(dirPath.getParent(), "file2"));
    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeys", 4L, omMetrics);
    assertCounter("NumCreateDirectory", 1L, omMetrics);
    assertCounter("NumKeyDeletes", 0L, omMetrics);
    assertCounter("NumKeyRenames", 0L, omMetrics);

    // Rename the child directory.
    fs.rename(dirPath, new Path(dirPath.getParent(), "new-name"));
    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeys", 4L, omMetrics);
    assertCounter("NumCreateDirectory", 1L, omMetrics);
    assertCounter("NumKeyDeletes", 0L, omMetrics);
    long expectedRenames = 1;
    if (bucketLayout == BucketLayout.LEGACY) {
      // Legacy bucket must rename keys individually.
      expectedRenames = 2;
    }
    assertCounter("NumKeyRenames", expectedRenames, omMetrics);

    // Delete metric should be decremented by directory deleting service in the background.
    fs.delete(dirPath.getParent(), true);
    GenericTestUtils.waitFor(() -> {
      long keyCount = MetricsAsserts.getLongCounter("NumKeys", getMetrics("OMMetrics"));
      return keyCount == 0;
    }, timeoutMillis / 5, timeoutMillis);
    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeys", 0L, omMetrics);
    // This is the number of times the create directory command was given, not the current number of directories.
    assertCounter("NumCreateDirectory", 1L, omMetrics);
    // Directory delete counts as key delete. One command was given so the metric is incremented once.
    assertCounter("NumKeyDeletes", 1L, omMetrics);
    assertCounter("NumKeyRenames", expectedRenames, omMetrics);

    // Re-create the same tree as before, but this time delete the bucket recursively.
    // All metrics should still be properly updated.
    fs.mkdirs(dirPath);
    ContractTestUtils.touch(fs, new Path(dirPath, "file1"));
    ContractTestUtils.touch(fs, new Path(dirPath.getParent(), "file2"));
    assertCounter("NumKeys", 4L, getMetrics("OMMetrics"));
    fs.delete(bucketPath, true);
    GenericTestUtils.waitFor(() -> {
      long keyCount = MetricsAsserts.getLongCounter("NumKeys", getMetrics("OMMetrics"));
      return keyCount == 0;
    }, timeoutMillis / 5, timeoutMillis);
    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeys", 0L, omMetrics);
    assertCounter("NumCreateDirectory", 2L, omMetrics);
    // One more keys delete request is given as part of the bucket delete to do a batch delete of its keys.
    assertCounter("NumKeyDeletes", 2L, omMetrics);
    assertCounter("NumKeyRenames", expectedRenames, omMetrics);
  }

  @Test
  public void testSnapshotOps() throws Exception {
    // This tests needs enough dataNodes to allocate the blocks for the keys.
    clusterBuilder.setNumDatanodes(3);
    startCluster();

    OmBucketInfo omBucketInfo = createBucketInfo(false);

    String volumeName = omBucketInfo.getVolumeName();
    String bucketName = omBucketInfo.getBucketName();
    String snapshot1 = "snap1";
    String snapshot2 = "snap2";

    writeClient.createBucket(omBucketInfo);

    // Create first key
    OmKeyArgs keyArgs1 = createKeyArgs(volumeName, bucketName,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
    OpenKeySession keySession = writeClient.openKey(keyArgs1);
    writeClient.commitKey(keyArgs1, keySession.getId());

    // Create first snapshot
    writeClient.createSnapshot(volumeName, bucketName, snapshot1);

    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");

    assertCounter("NumSnapshotCreateFails", 0L, omMetrics);
    assertCounter("NumSnapshotCreates", 1L, omMetrics);
    assertCounter("NumSnapshotListFails", 0L, omMetrics);
    assertCounter("NumSnapshotLists", 0L, omMetrics);
    assertCounter("NumSnapshotActive", 1L, omMetrics);
    assertCounter("NumSnapshotDeleted", 0L, omMetrics);
    assertCounter("NumSnapshotDiffJobs", 0L, omMetrics);
    assertCounter("NumSnapshotDiffJobFails", 0L, omMetrics);

    // Create second key
    OmKeyArgs keyArgs2 = createKeyArgs(volumeName, bucketName,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
    OpenKeySession keySession2 = writeClient.openKey(keyArgs2);
    writeClient.commitKey(keyArgs2, keySession2.getId());

    // Create second snapshot
    writeClient.createSnapshot(volumeName, bucketName, snapshot2);

    // Snapshot diff
    while (true) {
      SnapshotDiffResponse response =
          writeClient.snapshotDiff(volumeName, bucketName, snapshot1, snapshot2,
              null, 100, false, false);
      if (response.getJobStatus() == SnapshotDiffResponse.JobStatus.DONE) {
        break;
      } else {
        Thread.sleep(response.getWaitTimeInMs());
      }
    }
    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumSnapshotDiffJobs", 1L, omMetrics);
    assertCounter("NumSnapshotDiffJobFails", 0L, omMetrics);

    // List snapshots
    writeClient.listSnapshot(
        volumeName, bucketName, null, null, Integer.MAX_VALUE);

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumSnapshotActive", 2L, omMetrics);
    assertCounter("NumSnapshotCreates", 2L, omMetrics);
    assertCounter("NumSnapshotLists", 1L, omMetrics);
    assertCounter("NumSnapshotListFails", 0L, omMetrics);

    // List snapshot: invalid bucket case.
    assertThrows(OMException.class, () -> writeClient.listSnapshot(volumeName,
        "invalidBucket", null, null, Integer.MAX_VALUE));
    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumSnapshotLists", 2L, omMetrics);
    assertCounter("NumSnapshotListFails", 1L, omMetrics);

    // restart OM
    cluster.restartOzoneManager();

    // Check number of active snapshots in the snapshot table
    // is the same after OM restart
    assertCounter("NumSnapshotActive", 2L, omMetrics);
  }

  private <T> void mockWritePathExceptions(Class<T>klass) throws Exception {
    String tableName;
    if (klass == OmBucketInfo.class) {
      tableName = "bucketTable";
    } else {
      tableName = "volumeTable";
    }
    OMMetadataManager metadataManager = (OMMetadataManager)
        HddsWhiteboxTestUtils.getInternalState(ozoneManager, "metadataManager");
    OMMetadataManager mockMm = spy(metadataManager);
    @SuppressWarnings("unchecked")
    Table<String, T> table = (Table<String, T>)
        HddsWhiteboxTestUtils.getInternalState(metadataManager, tableName);
    Table<String, T> mockTable = spy(table);
    doThrow(exception).when(mockTable).isExist(any());
    if (klass == OmBucketInfo.class) {
      doReturn(mockTable).when(mockMm).getBucketTable();
    } else {
      doReturn(mockTable).when(mockMm).getVolumeTable();
    }
    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "metadataManager", mockMm);
  }

  @Test
  public void testAclOperations() throws Exception {
    startCluster();
    try {
      // Create a volume.
      client.getObjectStore().createVolume("volumeacl");

      OzoneObj volObj = new OzoneObjInfo.Builder().setVolumeName("volumeacl")
          .setResType(VOLUME).setStoreType(OZONE).build();

      // Test getAcl
      List<OzoneAcl> acls = ozoneManager.getAcl(volObj);
      MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
      assertCounter("NumGetAcl", 1L, omMetrics);

      // Test addAcl
      writeClient.addAcl(volObj,
          new OzoneAcl(IAccessAuthorizer.ACLIdentityType.USER, "ozoneuser",
              IAccessAuthorizer.ACLType.ALL, ACCESS));
      omMetrics = getMetrics("OMMetrics");
      assertCounter("NumAddAcl", 1L, omMetrics);

      // Test setAcl
      writeClient.setAcl(volObj, acls);
      omMetrics = getMetrics("OMMetrics");
      assertCounter("NumSetAcl", 1L, omMetrics);

      // Test removeAcl
      writeClient.removeAcl(volObj, acls.get(0));
      omMetrics = getMetrics("OMMetrics");
      assertCounter("NumRemoveAcl", 1L, omMetrics);

    } finally {
      client.getObjectStore().deleteVolume("volumeacl");
    }
  }

  @Test
  public void testAclOperationsHA() throws Exception {
    // This test needs a cluster with DNs and SCM to wait on safemode
    clusterBuilder.setNumDatanodes(3);
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED, true);
    startCluster();

    ObjectStore objectStore = client.getObjectStore();
    // Create a volume.
    objectStore.createVolume("volumeacl");
    // Create a bucket.
    objectStore.getVolume("volumeacl").createBucket("bucketacl");
    // Create a key.
    objectStore.getVolume("volumeacl").getBucket("bucketacl")
        .createKey("keyacl", 0).close();

    OzoneObj volObj =
        new OzoneObjInfo.Builder().setVolumeName("volumeacl").setResType(VOLUME)
            .setStoreType(OZONE).build();

    OzoneObj buckObj = new OzoneObjInfo.Builder().setVolumeName("volumeacl")
        .setBucketName("bucketacl").setResType(BUCKET).setStoreType(OZONE)
        .build();

    OzoneObj keyObj = new OzoneObjInfo.Builder().setVolumeName("volumeacl")
        .setBucketName("bucketacl").setResType(BUCKET).setKeyName("keyacl")
        .setStoreType(OZONE).build();

    List<OzoneAcl> acls = ozoneManager.getAcl(volObj);

    // Test Acl's for volume.
    testAclMetricsInternal(objectStore, volObj, acls);

    // Test Acl's for bucket.
    testAclMetricsInternal(objectStore, buckObj, acls);

    // Test Acl's for key.
    testAclMetricsInternal(objectStore, keyObj, acls);
  }

  private void testAclMetricsInternal(ObjectStore objectStore, OzoneObj volObj,
      List<OzoneAcl> acls) throws IOException {
    // Test addAcl
    OMMetrics metrics = ozoneManager.getMetrics();
    long initialValue = metrics.getNumAddAcl();
    objectStore.addAcl(volObj,
        new OzoneAcl(IAccessAuthorizer.ACLIdentityType.USER, "ozoneuser",
            IAccessAuthorizer.ACLType.ALL, ACCESS));

    assertEquals(initialValue + 1, metrics.getNumAddAcl());

    // Test setAcl
    initialValue = metrics.getNumSetAcl();

    objectStore.setAcl(volObj, acls);
    assertEquals(initialValue + 1, metrics.getNumSetAcl());

    // Test removeAcl
    initialValue = metrics.getNumRemoveAcl();
    objectStore.removeAcl(volObj, acls.get(0));
    assertEquals(initialValue + 1, metrics.getNumRemoveAcl());
  }

  /**
   * Test volume operations with ignoring thrown exception.
   */
  private void doVolumeOps(OmVolumeArgs volumeArgs) {
    try {
      writeClient.createVolume(volumeArgs);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.getVolumeInfo(volumeArgs.getVolume());
    } catch (IOException ignored) {
    }

    try {
      writeClient.setOwner(volumeArgs.getVolume(), "dummy");
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.listAllVolumes("", null, 0);
    } catch (IOException ignored) {
    }

    try {
      writeClient.deleteVolume(volumeArgs.getVolume());
    } catch (IOException ignored) {
    }
  }

  /**
   * Test bucket operations with ignoring thrown exception.
   */
  private void doBucketOps(OmBucketInfo info) {
    try {
      writeClient.createBucket(info);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.getBucketInfo(info.getVolumeName(), info.getBucketName());
    } catch (IOException ignored) {
    }

    try {
      writeClient.setBucketProperty(getBucketArgs(info));
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.listBuckets(info.getVolumeName(), null, null, 0, false);
    } catch (IOException ignored) {
    }

    try {
      writeClient.deleteBucket(info.getVolumeName(), info.getBucketName());
    } catch (IOException ignored) {
    }
  }

  /**
   * Test key operations with ignoring thrown exception.
   */
  private void doKeyOps(OmKeyArgs keyArgs) {
    OpenKeySession keySession = null;
    try {
      keySession = writeClient.openKey(keyArgs);
    } catch (IOException ignored) {
    }

    try {
      long id = (keySession != null) ? keySession.getId() : 0;
      writeClient.commitKey(keyArgs, id);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.lookupKey(keyArgs);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.listKeys(keyArgs.getVolumeName(),
                            keyArgs.getBucketName(), null, null, 0);
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.listTrash(keyArgs.getVolumeName(),
                             keyArgs.getBucketName(), null, null, 0);
    } catch (IOException ignored) {
    }

    try {
      writeClient.deleteKey(keyArgs);
    } catch (IOException ignored) {
    }

    try {
      writeClient.initiateMultipartUpload(keyArgs);
    } catch (IOException ignored) {
    }
  }

  private OmKeyArgs createKeyArgs(String volumeName, String bucketName,
      ReplicationConfig repConfig) throws IOException {
    OmKeyLocationInfo keyLocationInfo = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(new ContainerBlockID(1, 1)))
        .setPipeline(MockPipeline.createSingleNodePipeline())
        .build();
    keyLocationInfo.setCreateVersion(0);

    String keyName = UUID.randomUUID().toString();
    return new OmKeyArgs.Builder()
        .setLocationInfoList(Collections.singletonList(keyLocationInfo))
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setAcls(Lists.emptyList())
        .setReplicationConfig(repConfig)
        .build();
  }

  private OmVolumeArgs createVolumeArgs() {
    String volumeName = UUID.randomUUID().toString();
    return new OmVolumeArgs.Builder()
        .setVolume(volumeName)
        .setOwnerName("dummy")
        .setAdminName("dummyAdmin")
        .build();
  }
  private OmBucketArgs getBucketArgs(OmBucketInfo info) {
    return new OmBucketArgs.Builder()
        .setVolumeName(info.getVolumeName())
        .setBucketName(info.getBucketName())
        .build();
  }
  private OmBucketInfo createBucketInfo(boolean isEcBucket) throws IOException {
    OmVolumeArgs volumeArgs = createVolumeArgs();
    writeClient.createVolume(volumeArgs);
    DefaultReplicationConfig repConf = new DefaultReplicationConfig(
        new ECReplicationConfig("rs-3-2-1024k"));
    String bucketName = UUID.randomUUID().toString();

    OmBucketInfo.Builder builder = new OmBucketInfo.Builder()
        .setVolumeName(volumeArgs.getVolume())
        .setBucketName(bucketName);
    if (isEcBucket) {
      builder.setDefaultReplicationConfig(repConf);
    }
    return builder.build();
  }
}
