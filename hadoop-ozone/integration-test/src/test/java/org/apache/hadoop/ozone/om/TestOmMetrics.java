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
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

/**
 * Test for OM metrics.
 */
public class TestOmMetrics {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = Timeout.seconds(300);
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

  /**
   * Create a MiniDFSCluster for testing.
   */
  @Before
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
    writeClient = cluster.getRpcClient().getObjectStore()
        .getClientProxy().getOzoneManagerClient();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
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
    VolumeManager mockVm = Mockito.spy(volumeManager);

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
    Mockito.doThrow(exception).when(mockVm).getVolumeInfo(any());
    Mockito.doThrow(exception).when(mockVm).listVolumes(any(), any(),
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
    BucketManager mockBm = Mockito.spy(bucketManager);

    OmBucketInfo bucketInfo = createBucketInfo();
    doBucketOps(bucketInfo);

    MetricsRecordBuilder omMetrics = getMetrics("OMMetrics");
    assertCounter("NumBucketOps", 5L, omMetrics);
    assertCounter("NumBucketCreates", 1L, omMetrics);
    assertCounter("NumBucketUpdates", 1L, omMetrics);
    assertCounter("NumBucketInfos", 1L, omMetrics);
    assertCounter("NumBucketDeletes", 1L, omMetrics);
    assertCounter("NumBucketLists", 1L, omMetrics);
    assertCounter("NumBuckets", 0L, omMetrics);

    bucketInfo = createBucketInfo();
    writeClient.createBucket(bucketInfo);
    bucketInfo = createBucketInfo();
    writeClient.createBucket(bucketInfo);
    bucketInfo = createBucketInfo();
    writeClient.createBucket(bucketInfo);
    writeClient.deleteBucket(bucketInfo.getVolumeName(),
        bucketInfo.getBucketName());

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumBuckets", 2L, omMetrics);

    // inject exception to test for Failure Metrics on the read path
    Mockito.doThrow(exception).when(mockBm).getBucketInfo(any(), any());
    Mockito.doThrow(exception).when(mockBm).listBuckets(any(), any(),
        any(), anyInt());

    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "bucketManager", mockBm);

    // inject exception to test for Failure Metrics on the write path
    mockWritePathExceptions(OmBucketInfo.class);
    doBucketOps(bucketInfo);

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumBucketOps", 14L, omMetrics);
    assertCounter("NumBucketCreates", 5L, omMetrics);
    assertCounter("NumBucketUpdates", 2L, omMetrics);
    assertCounter("NumBucketInfos", 2L, omMetrics);
    assertCounter("NumBucketDeletes", 3L, omMetrics);
    assertCounter("NumBucketLists", 2L, omMetrics);

    assertCounter("NumBucketCreateFails", 1L, omMetrics);
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
    clusterBuilder.setNumDatanodes(3);
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED, true);
    startCluster();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    KeyManager keyManager = (KeyManager) HddsWhiteboxTestUtils
        .getInternalState(ozoneManager, "keyManager");
    KeyManager mockKm = Mockito.spy(keyManager);
    TestDataUtil.createVolumeAndBucket(cluster, volumeName, bucketName);
    OmKeyArgs keyArgs = createKeyArgs(volumeName, bucketName);
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

    keyArgs = createKeyArgs(volumeName, bucketName);
    OpenKeySession keySession = writeClient.openKey(keyArgs);
    writeClient.commitKey(keyArgs, keySession.getId());
    keyArgs = createKeyArgs(volumeName, bucketName);
    keySession = writeClient.openKey(keyArgs);
    writeClient.commitKey(keyArgs, keySession.getId());
    keyArgs = createKeyArgs(volumeName, bucketName);
    keySession = writeClient.openKey(keyArgs);
    writeClient.commitKey(keyArgs, keySession.getId());
    writeClient.deleteKey(keyArgs);


    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeys", 2L, omMetrics);

    // inject exception to test for Failure Metrics on the read path
    Mockito.doThrow(exception).when(mockKm).lookupKey(any(), any());
    Mockito.doThrow(exception).when(mockKm).listKeys(
        any(), any(), any(), any(), anyInt());
    Mockito.doThrow(exception).when(mockKm).listTrash(
        any(), any(), any(), any(), anyInt());
    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "keyManager", mockKm);

    // inject exception to test for Failure Metrics on the write path
    mockWritePathExceptions(OmBucketInfo.class);
    keyArgs = createKeyArgs(volumeName, bucketName);
    doKeyOps(keyArgs);

    omMetrics = getMetrics("OMMetrics");
    assertCounter("NumKeyOps", 21L, omMetrics);
    assertCounter("NumKeyAllocate", 5L, omMetrics);
    assertCounter("NumKeyLookup", 2L, omMetrics);
    assertCounter("NumKeyDeletes", 3L, omMetrics);
    assertCounter("NumKeyLists", 2L, omMetrics);
    assertCounter("NumTrashKeyLists", 2L, omMetrics);
    assertCounter("NumInitiateMultipartUploads", 2L, omMetrics);

    assertCounter("NumKeyAllocateFails", 1L, omMetrics);
    assertCounter("NumKeyLookupFails", 1L, omMetrics);
    assertCounter("NumKeyDeleteFails", 1L, omMetrics);
    assertCounter("NumKeyListFails", 1L, omMetrics);
    assertCounter("NumTrashKeyListFails", 1L, omMetrics);
    assertCounter("NumInitiateMultipartUploadFails", 1L, omMetrics);


    assertCounter("NumKeys", 2L, omMetrics);

    cluster.restartOzoneManager();
    assertCounter("NumKeys", 2L, omMetrics);

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
    OMMetadataManager mockMm = Mockito.spy(metadataManager);
    @SuppressWarnings("unchecked")
    Table<String, T> table = (Table<String, T>)
        HddsWhiteboxTestUtils.getInternalState(metadataManager, tableName);
    Table<String, T> mockTable = Mockito.spy(table);
    Mockito.doThrow(exception).when(mockTable).isExist(any());
    if (klass == OmBucketInfo.class) {
      Mockito.doReturn(mockTable).when(mockMm).getBucketTable();
    } else {
      Mockito.doReturn(mockTable).when(mockMm).getVolumeTable();
    }
    HddsWhiteboxTestUtils.setInternalState(
        ozoneManager, "metadataManager", mockMm);
  }

  @Test
  public void testAclOperations() throws Exception {
    startCluster();
    try {
      // Create a volume.
      cluster.getClient().getObjectStore().createVolume("volumeacl");

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
      cluster.getClient().getObjectStore().deleteVolume("volumeacl");
    }
  }

  @Test
  public void testAclOperationsHA() throws Exception {
    // This test needs a cluster with DNs and SCM to wait on safemode
    clusterBuilder.setNumDatanodes(3);
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED, true);
    startCluster();

    ObjectStore objectStore = cluster.getClient().getObjectStore();
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

    Assert.assertEquals(initialValue + 1, metrics.getNumAddAcl());

    // Test setAcl
    initialValue = metrics.getNumSetAcl();

    objectStore.setAcl(volObj, acls);
    Assert.assertEquals(initialValue + 1, metrics.getNumSetAcl());

    // Test removeAcl
    initialValue = metrics.getNumRemoveAcl();
    objectStore.removeAcl(volObj, acls.get(0));
    Assert.assertEquals(initialValue + 1, metrics.getNumRemoveAcl());
  }

  /**
   * Test volume operations with ignoring thrown exception.
   */
  private void doVolumeOps(OmVolumeArgs volumeArgs) {
    try {
      writeClient.createVolume(volumeArgs);
    } catch (IOException ignored) {
    }

    /*  Commenting this out for now since it seems wrong.
    try {
      writeClient.deleteVolume("nonExistentVolume");
    } catch (IOException ignored) {
    }
    */

    try {
      ozoneManager.getVolumeInfo(volumeArgs.getVolume());
    } catch (IOException ignored) {
    }

    /* This appears to have been removed from the VolumeManagerImpl:
       HDDS-4901.  Should I remove it from the OM?
    try {
      ozoneManager.checkVolumeAccess(null, null);
    } catch (IOException ignored) {
    }
    */

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
  private void doBucketOps(OmBucketInfo info) throws IOException{
    try {
      writeClient.createBucket(info);
    } catch (IOException ignored) {
    }

    /*  Commenting this out for now since it seems wrong.
    try {
      ozoneManager.deleteBucket(null, null);
    } catch (IOException ignored) {
    }
    */

    try {
      ozoneManager.getBucketInfo(info.getVolumeName(), info.getBucketName());
    } catch (IOException ignored) {
    }

    try {
      writeClient.setBucketProperty(getBucketArgs(info));
    } catch (IOException ignored) {
    }

    try {
      ozoneManager.listBuckets(info.getVolumeName(), null, null, 0);
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
      long id = (keySession != null)?keySession.getId():0;
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

  private OmKeyArgs createKeyArgs(String volumeName, String bucketName)
      throws IOException {
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
  private OmBucketInfo createBucketInfo() throws IOException {
    OmVolumeArgs volumeArgs = createVolumeArgs();
    writeClient.createVolume(volumeArgs);
    String bucketName = UUID.randomUUID().toString();
    return new OmBucketInfo.Builder()
        .setVolumeName(volumeArgs.getVolume())
        .setBucketName(bucketName)
        .build();
  }
}
