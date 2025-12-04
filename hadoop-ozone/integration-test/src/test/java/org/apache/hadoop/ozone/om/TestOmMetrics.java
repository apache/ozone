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

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_ROOT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.apache.ozone.test.MetricsAsserts.getLongCounter;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test for OM metrics.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
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

  @BeforeAll
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(OMConfigKeys.OZONE_OM_METRICS_SAVE_INTERVAL,
        1000, TimeUnit.MILLISECONDS);
    // Speed up background directory deletion for this test.
    conf.setTimeDuration(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 1000, TimeUnit.MILLISECONDS);
    // For testing fs operations with legacy buckets.
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    clusterBuilder = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5);
    startCluster();
  }

  private void startCluster() throws Exception {
    cluster = clusterBuilder.build();
    cluster.waitForClusterToBeReady();
    ozoneManager = cluster.getOzoneManager();
    client = cluster.newClient();
    writeClient = client.getObjectStore()
        .getClientProxy().getOzoneManagerClient();
  }

  @AfterAll
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testVolumeOps() throws Exception {
    VolumeManager volumeManager =
        (VolumeManager) HddsWhiteboxTestUtils.getInternalState(
            ozoneManager, "volumeManager");
    VolumeManager mockVm = spy(volumeManager);

    // get initial values for metrics
    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    long initialNumVolumeOps = getLongCounter("NumVolumeOps", omMetrics);
    long initialNumVolumeCreates = getLongCounter("NumVolumeCreates", omMetrics);
    long initialNumVolumeUpdates = getLongCounter("NumVolumeUpdates", omMetrics);
    long initialNumVolumeInfos = getLongCounter("NumVolumeInfos", omMetrics);
    long initialNumVolumeDeletes = getLongCounter("NumVolumeDeletes", omMetrics);
    long initialNumVolumeLists = getLongCounter("NumVolumeLists", omMetrics);
    long initialNumVolumes = getLongCounter("NumVolumes", omMetrics);

    long initialNumVolumeCreateFails = getLongCounter("NumVolumeCreateFails", omMetrics);
    long initialNumVolumeUpdateFails = getLongCounter("NumVolumeUpdateFails", omMetrics);
    long initialNumVolumeInfoFails = getLongCounter("NumVolumeInfoFails", omMetrics);
    long initialNumVolumeDeleteFails = getLongCounter("NumVolumeDeleteFails", omMetrics);
    long initialNumVolumeListFails = getLongCounter("NumVolumeListFails", omMetrics);

    OmVolumeArgs volumeArgs = createVolumeArgs();
    doVolumeOps(volumeArgs);

    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumVolumeOps + 5, getLongCounter("NumVolumeOps", omMetrics));
    assertEquals(initialNumVolumeCreates + 1, getLongCounter("NumVolumeCreates", omMetrics));
    assertEquals(initialNumVolumeUpdates + 1, getLongCounter("NumVolumeUpdates", omMetrics));
    assertEquals(initialNumVolumeInfos + 1, getLongCounter("NumVolumeInfos", omMetrics));
    assertEquals(initialNumVolumeDeletes + 1, getLongCounter("NumVolumeDeletes", omMetrics));
    assertEquals(initialNumVolumeLists + 1, getLongCounter("NumVolumeLists", omMetrics));
    assertEquals(initialNumVolumes, getLongCounter("NumVolumes", omMetrics));

    volumeArgs = createVolumeArgs();
    writeClient.createVolume(volumeArgs);
    volumeArgs = createVolumeArgs();
    writeClient.createVolume(volumeArgs);
    volumeArgs = createVolumeArgs();
    writeClient.createVolume(volumeArgs);
    writeClient.deleteVolume(volumeArgs.getVolume());

    omMetrics = getMetrics("OMMetrics");
    // Accounting 's3v' volume which is created by default.
    assertEquals(initialNumVolumes + 2, getLongCounter("NumVolumes", omMetrics));

    // inject exception to test for Failure Metrics on the read path
    doThrow(exception).when(mockVm).getVolumeInfo(any());
    doThrow(exception).when(mockVm).listVolumes(any(), any(),
        any(), anyInt());

    HddsWhiteboxTestUtils.setInternalState(ozoneManager,
        "volumeManager", mockVm);

    // inject exception to test for Failure Metrics on the write path
    OMMetadataManager metadataManager = mockWritePathExceptions(TestOmMetrics::mockVolumeTable);
    volumeArgs = createVolumeArgs();
    doVolumeOps(volumeArgs);

    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumVolumeOps + 14, getLongCounter("NumVolumeOps", omMetrics));
    assertEquals(initialNumVolumeCreates + 5, getLongCounter("NumVolumeCreates", omMetrics));
    assertEquals(initialNumVolumeUpdates + 2, getLongCounter("NumVolumeUpdates", omMetrics));
    assertEquals(initialNumVolumeInfos + 2, getLongCounter("NumVolumeInfos", omMetrics));
    assertEquals(initialNumVolumeDeletes + 3, getLongCounter("NumVolumeDeletes", omMetrics));
    assertEquals(initialNumVolumeLists + 2, getLongCounter("NumVolumeLists", omMetrics));

    assertEquals(initialNumVolumeCreateFails + 1, getLongCounter("NumVolumeCreateFails", omMetrics));
    assertEquals(initialNumVolumeUpdateFails + 1, getLongCounter("NumVolumeUpdateFails", omMetrics));
    assertEquals(initialNumVolumeInfoFails + 1, getLongCounter("NumVolumeInfoFails", omMetrics));
    assertEquals(initialNumVolumeDeleteFails + 1, getLongCounter("NumVolumeDeleteFails", omMetrics));
    assertEquals(initialNumVolumeListFails + 1, getLongCounter("NumVolumeListFails", omMetrics));
    assertEquals(initialNumVolumes + 2, getLongCounter("NumVolumes", omMetrics));

    // restore state
    HddsWhiteboxTestUtils.setInternalState(ozoneManager,
        "volumeManager", volumeManager);
    HddsWhiteboxTestUtils.setInternalState(ozoneManager,
        "metadataManager", metadataManager);
  }

  @Test
  public void testBucketOps() throws Exception {
    BucketManager bucketManager =
        (BucketManager) HddsWhiteboxTestUtils.getInternalState(
            ozoneManager, "bucketManager");
    BucketManager mockBm = spy(bucketManager);

    // get initial values for metrics
    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    long initialNumBucketOps = getLongCounter("NumBucketOps", omMetrics);
    long initialNumBucketCreates = getLongCounter("NumBucketCreates", omMetrics);
    long initialNumBucketUpdates = getLongCounter("NumBucketUpdates", omMetrics);
    long initialNumBucketInfos = getLongCounter("NumBucketInfos", omMetrics);
    long initialNumBucketDeletes = getLongCounter("NumBucketDeletes", omMetrics);
    long initialNumBucketLists = getLongCounter("NumBucketLists", omMetrics);
    long initialNumBuckets = getLongCounter("NumBuckets", omMetrics);
    long initialEcBucketCreateTotal = getLongCounter("EcBucketCreateTotal", omMetrics);
    long initialEcBucketCreateFailsTotal = getLongCounter("EcBucketCreateFailsTotal", omMetrics);

    long initialNumBucketCreateFails = getLongCounter("NumBucketCreateFails", omMetrics);
    long initialNumBucketUpdateFails = getLongCounter("NumBucketUpdateFails", omMetrics);
    long initialNumBucketInfoFails = getLongCounter("NumBucketInfoFails", omMetrics);
    long initialNumBucketDeleteFails = getLongCounter("NumBucketDeleteFails", omMetrics);
    long initialNumBucketListFails = getLongCounter("NumBucketListFails", omMetrics);

    OmBucketInfo bucketInfo = createBucketInfo(false);
    doBucketOps(bucketInfo);

    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumBucketOps + 5, getLongCounter("NumBucketOps", omMetrics));
    assertEquals(initialNumBucketCreates + 1, getLongCounter("NumBucketCreates", omMetrics));
    assertEquals(initialNumBucketUpdates + 1, getLongCounter("NumBucketUpdates", omMetrics));
    assertEquals(initialNumBucketInfos + 1, getLongCounter("NumBucketInfos", omMetrics));
    assertEquals(initialNumBucketDeletes + 1, getLongCounter("NumBucketDeletes", omMetrics));
    assertEquals(initialNumBucketLists + 1, getLongCounter("NumBucketLists", omMetrics));
    assertEquals(initialNumBuckets, getLongCounter("NumBuckets", omMetrics));

    OmBucketInfo ecBucketInfo = createBucketInfo(true);
    writeClient.createBucket(ecBucketInfo);
    writeClient.deleteBucket(ecBucketInfo.getVolumeName(),
        ecBucketInfo.getBucketName());

    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialEcBucketCreateTotal + 1, getLongCounter("EcBucketCreateTotal", omMetrics));

    bucketInfo = createBucketInfo(false);
    writeClient.createBucket(bucketInfo);
    bucketInfo = createBucketInfo(false);
    writeClient.createBucket(bucketInfo);
    bucketInfo = createBucketInfo(false);
    writeClient.createBucket(bucketInfo);
    writeClient.deleteBucket(bucketInfo.getVolumeName(),
        bucketInfo.getBucketName());

    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumBuckets + 2, getLongCounter("NumBuckets", omMetrics));

    // inject exception to test for Failure Metrics on the read path
    doThrow(exception).when(mockBm).getBucketInfo(any(), any());
    doThrow(exception).when(mockBm).listBuckets(any(), any(),
        any(), anyInt(), eq(false));

    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "bucketManager", mockBm);

    // inject exception to test for Failure Metrics on the write path
    OMMetadataManager metadataManager = mockWritePathExceptions(TestOmMetrics::mockBucketTable);
    doBucketOps(bucketInfo);

    ecBucketInfo = createBucketInfo(true);
    try {
      writeClient.createBucket(ecBucketInfo);
    } catch (Exception e) {
      //Expected failure
    }
    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialEcBucketCreateFailsTotal + 1, getLongCounter("EcBucketCreateFailsTotal", omMetrics));
    assertEquals(initialNumBucketOps + 17, getLongCounter("NumBucketOps", omMetrics));
    assertEquals(initialNumBucketCreates + 7, getLongCounter("NumBucketCreates", omMetrics));
    assertEquals(initialNumBucketUpdates + 2, getLongCounter("NumBucketUpdates", omMetrics));
    assertEquals(initialNumBucketInfos + 2, getLongCounter("NumBucketInfos", omMetrics));
    assertEquals(initialNumBucketDeletes + 4, getLongCounter("NumBucketDeletes", omMetrics));
    assertEquals(initialNumBucketLists + 2, getLongCounter("NumBucketLists", omMetrics));

    assertEquals(initialNumBucketCreateFails + 2, getLongCounter("NumBucketCreateFails", omMetrics));
    assertEquals(initialNumBucketUpdateFails + 1, getLongCounter("NumBucketUpdateFails", omMetrics));
    assertEquals(initialNumBucketInfoFails + 1, getLongCounter("NumBucketInfoFails", omMetrics));
    assertEquals(initialNumBucketDeleteFails + 1, getLongCounter("NumBucketDeleteFails", omMetrics));
    assertEquals(initialNumBucketListFails + 1, getLongCounter("NumBucketListFails", omMetrics));
    assertEquals(initialNumBuckets + 2, getLongCounter("NumBuckets", omMetrics));

    // restore state
    HddsWhiteboxTestUtils.setInternalState(ozoneManager,
        "bucketManager", bucketManager);
    HddsWhiteboxTestUtils.setInternalState(ozoneManager,
        "metadataManager", metadataManager);
  }

  @Test
  public void testKeyOps() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    KeyManager keyManager = (KeyManager) HddsWhiteboxTestUtils
        .getInternalState(ozoneManager, "keyManager");
    KeyManager mockKm = spy(keyManager);

    // get initial values for metrics
    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    long initialNumKeyOps = getLongCounter("NumKeyOps", omMetrics);
    long initialNumKeyAllocate = getLongCounter("NumKeyAllocate", omMetrics);
    long initialNumKeyLookup = getLongCounter("NumKeyLookup", omMetrics);
    long initialNumKeyDeletes = getLongCounter("NumKeyDeletes", omMetrics);
    long initialNumKeyLists = getLongCounter("NumKeyLists", omMetrics);
    long initialNumKeys = getLongCounter("NumKeys", omMetrics);
    long initialNumInitiateMultipartUploads = getLongCounter("NumInitiateMultipartUploads", omMetrics);
    long initialNumGetObjectTagging = getLongCounter("NumGetObjectTagging", omMetrics);
    long initialNumPutObjectTagging = getLongCounter("NumPutObjectTagging", omMetrics);
    long initialNumDeleteObjectTagging = getLongCounter("NumDeleteObjectTagging", omMetrics);

    long initialEcKeyCreateTotal = getLongCounter("EcKeyCreateTotal", omMetrics);
    long initialNumKeyAllocateFails = getLongCounter("NumKeyAllocateFails", omMetrics);
    long initialNumKeyLookupFails = getLongCounter("NumKeyLookupFails", omMetrics);
    long initialNumKeyDeleteFails = getLongCounter("NumKeyDeleteFails", omMetrics);
    long initialNumInitiateMultipartUploadFails = getLongCounter("NumInitiateMultipartUploadFails", omMetrics);
    long initialNumBlockAllocationFails = getLongCounter("NumBlockAllocationFails", omMetrics);
    long initialNumKeyListFails = getLongCounter("NumKeyListFails", omMetrics);
    long initialEcKeyCreateFailsTotal = getLongCounter("EcKeyCreateFailsTotal", omMetrics);
    long initialNumGetObjectTaggingFails = getLongCounter("NumGetObjectTaggingFails", omMetrics);
    long initialNumPutObjectTaggingFails = getLongCounter("NumPutObjectTaggingFails", omMetrics);
    long initialNumDeleteObjectTaggingFails = getLongCounter("NumDeleteObjectTaggingFails", omMetrics);

    // see HDDS-10078 for making this work with FILE_SYSTEM_OPTIMIZED layout
    TestDataUtil.createVolumeAndBucket(client, volumeName, bucketName, BucketLayout.LEGACY);
    OmKeyArgs keyArgs = createKeyArgs(volumeName, bucketName,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
    doKeyOps(keyArgs); // This will perform 7 different operations on the key

    omMetrics = getMetrics("OMMetrics");

    assertEquals(initialNumKeyOps + 10, getLongCounter("NumKeyOps", omMetrics));
    assertEquals(initialNumKeyAllocate + 1, getLongCounter("NumKeyAllocate", omMetrics));
    assertEquals(initialNumKeyLookup + 1, getLongCounter("NumKeyLookup", omMetrics));
    assertEquals(initialNumKeyDeletes + 1, getLongCounter("NumKeyDeletes", omMetrics));
    assertEquals(initialNumKeyLists + 1, getLongCounter("NumKeyLists", omMetrics));
    assertEquals(initialNumKeys, getLongCounter("NumKeys", omMetrics));
    assertEquals(initialNumInitiateMultipartUploads + 1, getLongCounter("NumInitiateMultipartUploads", omMetrics));
    assertEquals(initialNumGetObjectTagging + 1, getLongCounter("NumGetObjectTagging", omMetrics));
    assertEquals(initialNumPutObjectTagging + 1, getLongCounter("NumPutObjectTagging", omMetrics));
    assertEquals(initialNumDeleteObjectTagging + 1, getLongCounter("NumDeleteObjectTagging", omMetrics));

    keyArgs = createKeyArgs(volumeName, bucketName,
        new ECReplicationConfig("rs-3-2-1024K"));
    doKeyOps(keyArgs);

    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumKeys, getLongCounter("NumKeys", omMetrics));
    assertEquals(initialEcKeyCreateTotal + 1, getLongCounter("EcKeyCreateTotal", omMetrics));

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
    assertEquals(initialNumKeys + 2, getLongCounter("NumKeys", omMetrics));
    assertEquals(initialNumBlockAllocationFails + 1, getLongCounter("NumBlockAllocationFails", omMetrics));

    // inject exception to test for Failure Metrics on the read path
    doThrow(exception).when(mockKm).lookupKey(any(), any(), any());
    doThrow(exception).when(mockKm).listKeys(
        any(), any(), any(), any(), anyInt());
    doThrow(exception).when(mockKm).getObjectTagging(any(), any());
    OmMetadataReader omMetadataReader =
        (OmMetadataReader) ozoneManager.getOmMetadataReader().get();
    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "keyManager", mockKm);

    HddsWhiteboxTestUtils.setInternalState(
        omMetadataReader, "keyManager", mockKm);

    // inject exception to test for Failure Metrics on the write path
    OMMetadataManager metadataManager = mockWritePathExceptions(TestOmMetrics::mockBucketTable);
    keyArgs = createKeyArgs(volumeName, bucketName,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
    doKeyOps(keyArgs);

    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumKeyOps + 37, getLongCounter("NumKeyOps", omMetrics));
    assertEquals(initialNumKeyAllocate + 6, getLongCounter("NumKeyAllocate", omMetrics));
    assertEquals(initialNumKeyLookup + 3, getLongCounter("NumKeyLookup", omMetrics));
    assertEquals(initialNumKeyDeletes + 4, getLongCounter("NumKeyDeletes", omMetrics));
    assertEquals(initialNumKeyLists + 3, getLongCounter("NumKeyLists", omMetrics));
    assertEquals(initialNumInitiateMultipartUploads + 3, getLongCounter("NumInitiateMultipartUploads", omMetrics));

    assertEquals(initialNumKeyAllocateFails + 1, getLongCounter("NumKeyAllocateFails", omMetrics));
    assertEquals(initialNumKeyLookupFails + 1, getLongCounter("NumKeyLookupFails", omMetrics));
    assertEquals(initialNumKeyDeleteFails + 1, getLongCounter("NumKeyDeleteFails", omMetrics));
    assertEquals(initialNumKeyListFails + 1, getLongCounter("NumKeyListFails", omMetrics));
    assertEquals(initialNumInitiateMultipartUploadFails + 1, getLongCounter(
        "NumInitiateMultipartUploadFails", omMetrics));
    assertEquals(initialNumKeys + 2, getLongCounter("NumKeys", omMetrics));
    assertEquals(initialNumGetObjectTaggingFails + 1,  getLongCounter("NumGetObjectTaggingFails", omMetrics));
    assertEquals(initialNumPutObjectTaggingFails + 1, getLongCounter("NumPutObjectTaggingFails", omMetrics));
    assertEquals(initialNumDeleteObjectTaggingFails + 1, getLongCounter("NumDeleteObjectTaggingFails", omMetrics));

    keyArgs = createKeyArgs(volumeName, bucketName,
        new ECReplicationConfig("rs-3-2-1024K"));
    try {
      keySession = writeClient.openKey(keyArgs);
      writeClient.commitKey(keyArgs, keySession.getId());
    } catch (Exception e) {
      //Expected Failure
    }
    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialEcKeyCreateFailsTotal + 1, getLongCounter("EcKeyCreateFailsTotal", omMetrics));

    // restore state
    HddsWhiteboxTestUtils.setInternalState(ozoneManager,
        "keyManager", keyManager);
    HddsWhiteboxTestUtils.setInternalState(ozoneManager,
        "metadataManager", metadataManager);
  }

  @ParameterizedTest
  @EnumSource(value = BucketLayout.class, names = {"FILE_SYSTEM_OPTIMIZED", "LEGACY"})
  public void testDirectoryOps(BucketLayout bucketLayout) throws Exception {
    // get initial values for metrics
    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    long initialNumKeys = getLongCounter("NumKeys", omMetrics);
    long initialNumCreateDirectory = getLongCounter("NumCreateDirectory", omMetrics);
    long initialNumKeyDeletes = getLongCounter("NumKeyDeletes", omMetrics);
    long initialNumKeyRenames = getLongCounter("NumKeyRenames", omMetrics);

    // How long to wait for directory deleting service to clean up the files before aborting the test.
    final int timeoutMillis =
        (int)conf.getTimeDuration(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 0, TimeUnit.MILLISECONDS) * 3;
    assertTrue(timeoutMillis > 0, "Failed to read directory deleting service interval. Retrieved " + timeoutMillis +
        " milliseconds");

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    // create bucket with different layout in each ParameterizedTest
    TestDataUtil.createVolumeAndBucket(client, volumeName, bucketName, bucketLayout);

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
    assertEquals(initialNumKeys + 2, getLongCounter("NumKeys", omMetrics));
    // Only one directory create command is given, even though it created two directories.
    assertEquals(initialNumCreateDirectory + 1, getLongCounter("NumCreateDirectory", omMetrics));
    assertEquals(initialNumKeyDeletes, getLongCounter("NumKeyDeletes", omMetrics));
    assertEquals(initialNumKeyRenames, getLongCounter("NumKeyRenames", omMetrics));


    // Add 2 files at different parts of the tree.
    ContractTestUtils.touch(fs, new Path(dirPath, "file1"));
    ContractTestUtils.touch(fs, new Path(dirPath.getParent(), "file2"));
    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumKeys + 4, getLongCounter("NumKeys", omMetrics));
    assertEquals(initialNumCreateDirectory + 1, getLongCounter("NumCreateDirectory", omMetrics));
    assertEquals(initialNumKeyDeletes, getLongCounter("NumKeyDeletes", omMetrics));
    assertEquals(initialNumKeyRenames, getLongCounter("NumKeyRenames", omMetrics));

    // Rename the child directory.
    fs.rename(dirPath, new Path(dirPath.getParent(), "new-name"));
    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumKeys + 4, getLongCounter("NumKeys", omMetrics));
    assertEquals(initialNumCreateDirectory + 1, getLongCounter("NumCreateDirectory", omMetrics));
    assertEquals(initialNumKeyDeletes, getLongCounter("NumKeyDeletes", omMetrics));
    long expectedRenames = 1;
    if (bucketLayout == BucketLayout.LEGACY) {
      // Legacy bucket must rename keys individually.
      expectedRenames = 2;
    }
    assertEquals(initialNumKeyRenames + expectedRenames, getLongCounter("NumKeyRenames", omMetrics));

    // Delete metric should be decremented by directory deleting service in the background.
    fs.delete(dirPath.getParent(), true);
    GenericTestUtils.waitFor(() -> {
      long keyCount = getLongCounter("NumKeys", getMetrics("OMMetrics"));
      return keyCount == 0;
    }, timeoutMillis / 5, timeoutMillis);
    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumKeys, getLongCounter("NumKeys", omMetrics));
    // This is the number of times the create directory command was given, not the current number of directories.
    assertEquals(initialNumCreateDirectory + 1, getLongCounter("NumCreateDirectory", omMetrics));
    // Directory delete counts as key delete. One command was given so the metric is incremented once.
    assertEquals(initialNumKeyDeletes + 1, getLongCounter("NumKeyDeletes", omMetrics));
    assertEquals(initialNumKeyRenames + expectedRenames, getLongCounter("NumKeyRenames", omMetrics));

    // Re-create the same tree as before, but this time delete the bucket recursively.
    // All metrics should still be properly updated.
    fs.mkdirs(dirPath);
    ContractTestUtils.touch(fs, new Path(dirPath, "file1"));
    ContractTestUtils.touch(fs, new Path(dirPath.getParent(), "file2"));
    assertEquals(initialNumKeys, getLongCounter("NumKeys", omMetrics));
    fs.delete(bucketPath, true);
    GenericTestUtils.waitFor(() -> {
      long keyCount = getLongCounter("NumKeys", getMetrics("OMMetrics"));
      return keyCount == 0;
    }, timeoutMillis / 5, timeoutMillis);
    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumKeys, getLongCounter("NumKeys", omMetrics));
    assertEquals(initialNumCreateDirectory + 2, getLongCounter("NumCreateDirectory", omMetrics));
    // One more keys delete request is given as part of the bucket delete to do a batch delete of its keys.
    assertEquals(initialNumKeyDeletes + 2, getLongCounter("NumKeyDeletes", omMetrics));
    assertEquals(initialNumKeyRenames + expectedRenames, getLongCounter("NumKeyRenames", omMetrics));
  }

  @SuppressWarnings("checkstyle:methodlength")
  @Test
  public void testSnapshotOps() throws Exception {
    // This tests needs enough dataNodes to allocate the blocks for the keys.
    // get initial values for metrics
    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    long initialNumSnapshotCreateFails = getLongCounter("NumSnapshotCreateFails", omMetrics);
    long initialNumSnapshotCreates = getLongCounter("NumSnapshotCreates", omMetrics);
    long initialNumSnapshotInfoFails = getLongCounter("NumSnapshotInfoFails", omMetrics);
    long initialNumSnapshotInfos = getLongCounter("NumSnapshotInfos", omMetrics);
    long initialNumSnapshotListFails = getLongCounter("NumSnapshotListFails", omMetrics);
    long initialNumSnapshotLists = getLongCounter("NumSnapshotLists", omMetrics);
    long initialNumSnapshotActive = getLongCounter("NumSnapshotActive", omMetrics);
    long initialNumSnapshotDeleted = getLongCounter("NumSnapshotDeleted", omMetrics);
    long initialNumSnapshotDeletes = getLongCounter("NumSnapshotDeletes", omMetrics);
    long initialNumSnapshotDeleteFails = getLongCounter("NumSnapshotDeleteFails", omMetrics);
    long initialNumSnapshotDiffJobs = getLongCounter("NumSnapshotDiffJobs", omMetrics);
    long initialNumSnapshotDiffJobFails = getLongCounter("NumSnapshotDiffJobFails", omMetrics);
    long initialNumListSnapshotDiffJobs = getLongCounter("NumListSnapshotDiffJobs", omMetrics);
    long initialNumListSnapshotDiffJobFails = getLongCounter("NumListSnapshotDiffJobFails", omMetrics);
    long initialNumCancelSnapshotDiffs = getLongCounter("NumCancelSnapshotDiffs", omMetrics);
    long initialNumCancelSnapshotDiffFails = getLongCounter("NumCancelSnapshotDiffFails", omMetrics);
    long initialNumSnapshotRenames = getLongCounter("NumSnapshotRenames", omMetrics);
    long initialNumSnapshotRenameFails = getLongCounter("NumSnapshotRenameFails", omMetrics);

    OmBucketInfo omBucketInfo = createBucketInfo(false);

    String volumeName = omBucketInfo.getVolumeName();
    String bucketName = omBucketInfo.getBucketName();
    String snapshot1 = "snap1";
    String snapshot2 = "snap2";
    String snapshot3 = "snap3";

    writeClient.createBucket(omBucketInfo);

    // Create first key
    OmKeyArgs keyArgs1 = createKeyArgs(volumeName, bucketName,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
    OpenKeySession keySession = writeClient.openKey(keyArgs1);
    writeClient.commitKey(keyArgs1, keySession.getId());

    // Create first snapshot
    writeClient.createSnapshot(volumeName, bucketName, snapshot1);

    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumSnapshotCreateFails, getLongCounter("NumSnapshotCreateFails", omMetrics));
    assertEquals(initialNumSnapshotCreates + 1, getLongCounter("NumSnapshotCreates", omMetrics));
    assertEquals(initialNumSnapshotListFails, getLongCounter("NumSnapshotListFails", omMetrics));
    assertEquals(initialNumSnapshotLists, getLongCounter("NumSnapshotLists", omMetrics));
    assertEquals(initialNumSnapshotActive + 1, getLongCounter("NumSnapshotActive", omMetrics));
    assertEquals(initialNumSnapshotDeleted, getLongCounter("NumSnapshotDeleted", omMetrics));
    assertEquals(initialNumSnapshotDiffJobs, getLongCounter("NumSnapshotDiffJobs", omMetrics));
    assertEquals(initialNumSnapshotDiffJobFails, getLongCounter("NumSnapshotDiffJobFails", omMetrics));

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

    assertEquals(initialNumSnapshotDiffJobs + 1, getLongCounter("NumSnapshotDiffJobs", omMetrics));
    assertEquals(initialNumSnapshotDiffJobFails, getLongCounter("NumSnapshotDiffJobFails", omMetrics));

    // List snapshot diff jobs
    writeClient.listSnapshotDiffJobs(volumeName, bucketName, "", true, null, 1000);

    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumListSnapshotDiffJobs + 1, getLongCounter("NumListSnapshotDiffJobs", omMetrics));
    assertEquals(initialNumListSnapshotDiffJobFails, getLongCounter("NumListSnapshotDiffJobFails", omMetrics));

    // List snapshot diff jobs: invalid bucket case.
    assertThrows(OMException.class, () ->
        writeClient.listSnapshotDiffJobs(volumeName, "invalidBucket", "", true, null, 1000));
    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumListSnapshotDiffJobs + 2, getLongCounter("NumListSnapshotDiffJobs", omMetrics));
    assertEquals(initialNumListSnapshotDiffJobFails + 1, getLongCounter("NumListSnapshotDiffJobFails", omMetrics));

    // Cancel snapshot diff
    writeClient.cancelSnapshotDiff(volumeName, bucketName, snapshot1, snapshot2);

    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumCancelSnapshotDiffs + 1, getLongCounter("NumCancelSnapshotDiffs", omMetrics));
    assertEquals(initialNumCancelSnapshotDiffFails, getLongCounter("NumCancelSnapshotDiffFails", omMetrics));

    // Cancel snapshot diff job: invalid bucket case.
    assertThrows(OMException.class, () ->
        writeClient.cancelSnapshotDiff(volumeName, "invalidBucket", snapshot1, snapshot2));
    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumCancelSnapshotDiffs + 2, getLongCounter("NumCancelSnapshotDiffs", omMetrics));
    assertEquals(initialNumCancelSnapshotDiffFails + 1, getLongCounter("NumCancelSnapshotDiffFails", omMetrics));

    // Get snapshot info
    writeClient.getSnapshotInfo(volumeName, bucketName, snapshot1);

    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumSnapshotInfos + 1, getLongCounter("NumSnapshotInfos", omMetrics));
    assertEquals(initialNumSnapshotInfoFails, getLongCounter("NumSnapshotInfoFails", omMetrics));

    // Get snapshot info: invalid snapshot case.
    assertThrows(OMException.class, () ->
        writeClient.getSnapshotInfo(volumeName, bucketName, "invalidSnapshot"));
    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumSnapshotInfos + 2, getLongCounter("NumSnapshotInfos", omMetrics));
    assertEquals(initialNumSnapshotInfoFails + 1, getLongCounter("NumSnapshotInfoFails", omMetrics));

    // List snapshots
    writeClient.listSnapshot(
        volumeName, bucketName, null, null, Integer.MAX_VALUE);

    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumSnapshotActive + 2, getLongCounter("NumSnapshotActive", omMetrics));
    assertEquals(initialNumSnapshotCreates + 2, getLongCounter("NumSnapshotCreates", omMetrics));
    assertEquals(initialNumSnapshotListFails, getLongCounter("NumSnapshotListFails", omMetrics));
    assertEquals(initialNumSnapshotLists + 1, getLongCounter("NumSnapshotLists", omMetrics));

    // List snapshot: invalid bucket case.
    assertThrows(OMException.class, () -> writeClient.listSnapshot(volumeName,
        "invalidBucket", null, null, Integer.MAX_VALUE));
    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumSnapshotLists + 2, getLongCounter("NumSnapshotLists", omMetrics));
    assertEquals(initialNumSnapshotListFails + 1, getLongCounter("NumSnapshotListFails", omMetrics));

    // Rename snapshot
    writeClient.renameSnapshot(volumeName, bucketName, snapshot2, snapshot3);

    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumSnapshotActive + 2, getLongCounter("NumSnapshotActive", omMetrics));
    assertEquals(initialNumSnapshotRenames + 1, getLongCounter("NumSnapshotRenames", omMetrics));
    assertEquals(initialNumSnapshotRenameFails, getLongCounter("NumSnapshotRenameFails", omMetrics));

    // Rename snapshot: invalid snapshot case.
    assertThrows(OMException.class, () -> writeClient.renameSnapshot(volumeName,
        bucketName, snapshot2, snapshot3));
    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumSnapshotActive + 2, getLongCounter("NumSnapshotActive", omMetrics));
    assertEquals(initialNumSnapshotRenames + 2, getLongCounter("NumSnapshotRenames", omMetrics));
    assertEquals(initialNumSnapshotRenameFails + 1, getLongCounter("NumSnapshotRenameFails", omMetrics));

    // Delete snapshot
    writeClient.deleteSnapshot(volumeName, bucketName, snapshot3);

    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumSnapshotActive + 1, getLongCounter("NumSnapshotActive", omMetrics));
    assertEquals(initialNumSnapshotDeletes + 1, getLongCounter("NumSnapshotDeletes", omMetrics));
    assertEquals(initialNumSnapshotDeleted + 1, getLongCounter("NumSnapshotDeleted", omMetrics));
    assertEquals(initialNumSnapshotDeleteFails, getLongCounter("NumSnapshotDeleteFails", omMetrics));

    // Delete snapshot: invalid snapshot case.
    assertThrows(OMException.class, () -> writeClient.deleteSnapshot(volumeName,
        bucketName, snapshot3));
    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumSnapshotActive + 1, getLongCounter("NumSnapshotActive", omMetrics));
    assertEquals(initialNumSnapshotDeletes + 2, getLongCounter("NumSnapshotDeletes", omMetrics));
    assertEquals(initialNumSnapshotDeleted + 1, getLongCounter("NumSnapshotDeleted", omMetrics));
    assertEquals(initialNumSnapshotDeleteFails + 1, getLongCounter("NumSnapshotDeleteFails", omMetrics));
  }

  private OMMetadataManager mockWritePathExceptions(
      Function<OMMetadataManager, Table<String, ?>> getTable
  ) throws Exception {
    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    OMMetadataManager spy = spy(metadataManager);
    Table<String, ?> table = getTable.apply(spy);
    doThrow(new RocksDatabaseException()).when(table).isExist(any());
    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "metadataManager", spy);

    // Return the original metadataManager so it can be restored later
    return metadataManager;
  }

  private static Table<String, OmVolumeArgs> mockVolumeTable(OMMetadataManager spy) {
    Table<String, OmVolumeArgs> table = spy(spy.getVolumeTable());
    when(spy.getVolumeTable())
        .thenReturn(table);
    return table;
  }

  private static Table<String, OmBucketInfo> mockBucketTable(OMMetadataManager spy) {
    Table<String, OmBucketInfo> table = spy(spy.getBucketTable());
    when(spy.getBucketTable())
        .thenReturn(table);
    return table;
  }

  @Test
  public void testAclOperations() throws Exception {
    // get initial values for metrics
    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    long initialNumGetAcl = getLongCounter("NumGetAcl", omMetrics);
    long initialNumAddAcl = getLongCounter("NumAddAcl", omMetrics);
    long initialNumSetAcl = getLongCounter("NumSetAcl", omMetrics);
    long initialNumRemoveAcl = getLongCounter("NumRemoveAcl", omMetrics);
    // Create a volume.
    client.getObjectStore().createVolume("volumeacl");

    OzoneObj volObj = new OzoneObjInfo.Builder().setVolumeName("volumeacl")
        .setResType(VOLUME).setStoreType(OZONE).build();

    // Test getAcl, addAcl, setAcl, removeAcl
    List<OzoneAcl> acls = ozoneManager.getAcl(volObj);
    writeClient.addAcl(volObj,
        OzoneAcl.of(IAccessAuthorizer.ACLIdentityType.USER, "ozoneuser",
            ACCESS, IAccessAuthorizer.ACLType.ALL));
    writeClient.setAcl(volObj, acls);
    writeClient.removeAcl(volObj, acls.get(0));

    omMetrics = getMetrics("OMMetrics");
    assertEquals(initialNumGetAcl + 1, getLongCounter("NumGetAcl", omMetrics));
    assertEquals(initialNumAddAcl + 1, getLongCounter("NumAddAcl", omMetrics));
    assertEquals(initialNumSetAcl + 1, getLongCounter("NumSetAcl", omMetrics));
    assertEquals(initialNumRemoveAcl + 1, getLongCounter("NumRemoveAcl", omMetrics));

    client.getObjectStore().deleteVolume("volumeacl");
  }

  @Test
  public void testAclOperationsHA() throws Exception {
    ObjectStore objectStore = client.getObjectStore();
    // Create a volume.
    objectStore.createVolume("volumeaclha");
    // Create a bucket.
    objectStore.getVolume("volumeaclha").createBucket("bucketaclha");
    // Create a key.
    objectStore.getVolume("volumeaclha").getBucket("bucketaclha")
        .createKey("keyaclha", 0).close();

    OzoneObj volObj =
        new OzoneObjInfo.Builder().setVolumeName("volumeaclha").setResType(VOLUME)
            .setStoreType(OZONE).build();

    OzoneObj buckObj = new OzoneObjInfo.Builder().setVolumeName("volumeaclha")
        .setBucketName("bucketaclha").setResType(BUCKET).setStoreType(OZONE)
        .build();

    OzoneObj keyObj = new OzoneObjInfo.Builder().setVolumeName("volumeaclha")
        .setBucketName("bucketaclha").setResType(BUCKET).setKeyName("keyaclha")
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
        OzoneAcl.of(IAccessAuthorizer.ACLIdentityType.USER, "ozoneuser",
            ACCESS, IAccessAuthorizer.ACLType.ALL));

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
      writeClient.putObjectTagging(keyArgs);
    } catch (IOException ignored) {
    }

    try {
      writeClient.getObjectTagging(keyArgs);
    } catch (IOException ignored) {
    }

    try {
      writeClient.deleteObjectTagging(keyArgs);
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

    try {
      writeClient.listOpenFiles("", 100, "");
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
        .setOwnerName(UserGroupInformation.getCurrentUser().getShortUserName())
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
