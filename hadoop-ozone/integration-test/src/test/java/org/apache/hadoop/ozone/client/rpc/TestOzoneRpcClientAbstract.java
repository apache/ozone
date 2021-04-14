/**
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

package org.apache.hadoop.ozone.client.rpc;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmFailoverProxyUtil;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.OMProxyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneAclConfig;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import static java.nio.charset.StandardCharsets.UTF_8;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import static org.apache.hadoop.hdds.StringUtils.string2Bytes;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.client.ReplicationType.STAND_ALONE;
import static org.apache.hadoop.ozone.OmUtils.MAX_TRXN_ID;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.DEFAULT_OM_UPDATE_ID;
import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PARTIAL_RENAME;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.GROUP;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import org.junit.Assert;

import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * This is an abstract class to test all the public facing APIs of Ozone
 * Client, w/o OM Ratis server.
 * {@link TestOzoneRpcClient} tests the Ozone Client by submitting the
 * requests directly to OzoneManager. {@link TestOzoneRpcClientWithRatis}
 * tests the Ozone Client by submitting requests to OM's Ratis server.
 */
public abstract class TestOzoneRpcClientAbstract {

  private static MiniOzoneCluster cluster = null;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  private static OzoneManager ozoneManager;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static String remoteUserName = "remoteUser";
  private static String remoteGroupName = "remoteGroup";
  private static OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
      READ, DEFAULT);
  private static OzoneAcl defaultGroupAcl = new OzoneAcl(GROUP, remoteGroupName,
      READ, DEFAULT);
  private static OzoneAcl inheritedUserAcl = new OzoneAcl(USER, remoteUserName,
      READ, ACCESS);
  private static OzoneAcl inheritedGroupAcl = new OzoneAcl(GROUP,
      remoteGroupName, READ, ACCESS);

  private static String scmId = UUID.randomUUID().toString();
  private static String clusterId = UUID.randomUUID().toString();


  /**
   * Create a MiniOzoneCluster for testing.
   * @param conf Configurations to start the cluster.
   * @throws Exception
   */
  static void startCluster(OzoneConfiguration conf) throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setTotalPipelineNumLimit(10)
        .setScmId(scmId)
        .setClusterId(clusterId)
        .build();
    cluster.waitForClusterToBeReady();
    ozClient = OzoneClientFactory.getRpcClient(conf);
    store = ozClient.getObjectStore();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
    ozoneManager = cluster.getOzoneManager();
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  static void shutdownCluster() throws IOException {
    if(ozClient != null) {
      ozClient.close();
    }

    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public static void setCluster(MiniOzoneCluster cluster) {
    TestOzoneRpcClientAbstract.cluster = cluster;
  }

  public static void setOzClient(OzoneClient ozClient) {
    TestOzoneRpcClientAbstract.ozClient = ozClient;
  }

  public static void setOzoneManager(OzoneManager ozoneManager){
    TestOzoneRpcClientAbstract.ozoneManager = ozoneManager;
  }

  public static void setStorageContainerLocationClient(
      StorageContainerLocationProtocolClientSideTranslatorPB
          storageContainerLocationClient) {
    TestOzoneRpcClientAbstract.storageContainerLocationClient =
        storageContainerLocationClient;
  }

  public static void setStore(ObjectStore store) {
    TestOzoneRpcClientAbstract.store = store;
  }

  public static ObjectStore getStore() {
    return TestOzoneRpcClientAbstract.store;
  }

  public static void setClusterId(String clusterId) {
    TestOzoneRpcClientAbstract.clusterId = clusterId;
  }

  /**
   * Test OM Proxy Provider.
   */
  @Test
  public void testOMClientProxyProvider() {

    OMFailoverProxyProvider omFailoverProxyProvider =
        OmFailoverProxyUtil.getFailoverProxyProvider(store.getClientProxy());

    List<OMProxyInfo> omProxies = omFailoverProxyProvider.getOMProxyInfos();

    // For a non-HA OM service, there should be only one OM proxy.
    Assert.assertEquals(1, omProxies.size());
    // The address in OMProxyInfo object, which client will connect to,
    // should match the OM's RPC address.
    Assert.assertTrue(omProxies.get(0).getAddress().equals(
        ozoneManager.getOmRpcServerAddr()));
  }

  @Test
  public void testDefaultS3GVolumeExists() throws Exception {
    String s3VolumeName = HddsClientUtils.getS3VolumeName(cluster.getConf());
    OzoneVolume ozoneVolume = store.getVolume(s3VolumeName);
    Assert.assertEquals(ozoneVolume.getName(), s3VolumeName);
    OMMetadataManager omMetadataManager =
        cluster.getOzoneManager().getMetadataManager();
    long transactionID = MAX_TRXN_ID + 1;
    long objectID = OmUtils.addEpochToTxId(omMetadataManager.getOmEpoch(),
        transactionID);
    OmVolumeArgs omVolumeArgs =
        cluster.getOzoneManager().getMetadataManager().getVolumeTable().get(
            omMetadataManager.getVolumeKey(s3VolumeName));
    Assert.assertEquals(objectID, omVolumeArgs.getObjectID());
    Assert.assertEquals(DEFAULT_OM_UPDATE_ID, omVolumeArgs.getUpdateID());
  }

  @Test
  public void testVolumeSetOwner() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);

    String ownerName = "someRandomUser1";

    ClientProtocol proxy = store.getClientProxy();
    proxy.setVolumeOwner(volumeName, ownerName);
    // Set owner again
    proxy.setVolumeOwner(volumeName, ownerName);
  }

  @Test
  public void testSetAndClrQuota() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String bucketName2 = UUID.randomUUID().toString();
    String value = "sample value";
    int valueLength = value.getBytes(UTF_8).length;
    OzoneVolume volume = null;
    store.createVolume(volumeName);

    store.getVolume(volumeName).setQuota(OzoneQuota.parseQuota(
        "10GB", "10000"));
    store.getVolume(volumeName).createBucket(bucketName);
    volume = store.getVolume(volumeName);
    Assert.assertEquals(10 * GB, volume.getQuotaInBytes());
    Assert.assertEquals(10000L, volume.getQuotaInNamespace());
    OzoneBucket bucket = store.getVolume(volumeName).getBucket(bucketName);
    Assert.assertEquals(OzoneConsts.QUOTA_RESET, bucket.getQuotaInBytes());
    Assert.assertEquals(OzoneConsts.QUOTA_RESET, bucket.getQuotaInNamespace());

    store.getVolume(volumeName).getBucket(bucketName).setQuota(
        OzoneQuota.parseQuota("1GB", "1000"));
    OzoneBucket ozoneBucket = store.getVolume(volumeName).getBucket(bucketName);
    Assert.assertEquals(1024 * 1024 * 1024,
        ozoneBucket.getQuotaInBytes());
    Assert.assertEquals(1000L, ozoneBucket.getQuotaInNamespace());

    store.getVolume(volumeName).createBucket(bucketName2);
    store.getVolume(volumeName).getBucket(bucketName2).setQuota(
        OzoneQuota.parseQuota("1024", "1000"));
    OzoneBucket ozoneBucket2 =
        store.getVolume(volumeName).getBucket(bucketName2);
    Assert.assertEquals(1024L, ozoneBucket2.getQuotaInBytes());

    LambdaTestUtils.intercept(IOException.class, "Can not clear bucket" +
        " spaceQuota because volume spaceQuota is not cleared.",
        () -> ozoneBucket.clearSpaceQuota());

    writeKey(bucket, UUID.randomUUID().toString(), ONE, value, valueLength);
    Assert.assertEquals(1L,
        store.getVolume(volumeName).getBucket(bucketName).getUsedNamespace());
    Assert.assertEquals(valueLength,
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());
    Assert.assertEquals(2L,
        store.getVolume(volumeName).getUsedNamespace());

    store.getVolume(volumeName).clearSpaceQuota();
    store.getVolume(volumeName).clearNamespaceQuota();
    OzoneVolume clrVolume = store.getVolume(volumeName);
    Assert.assertEquals(OzoneConsts.QUOTA_RESET, clrVolume.getQuotaInBytes());
    Assert.assertEquals(OzoneConsts.QUOTA_RESET,
        clrVolume.getQuotaInNamespace());

    ozoneBucket.clearSpaceQuota();
    ozoneBucket.clearNamespaceQuota();
    OzoneBucket clrBucket = store.getVolume(volumeName).getBucket(bucketName);
    Assert.assertEquals(OzoneConsts.QUOTA_RESET, clrBucket.getQuotaInBytes());
    Assert.assertEquals(OzoneConsts.QUOTA_RESET,
        clrBucket.getQuotaInNamespace());
    Assert.assertEquals(1L,
        store.getVolume(volumeName).getBucket(bucketName).getUsedNamespace());
    Assert.assertEquals(valueLength,
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());
    Assert.assertEquals(2L,
        store.getVolume(volumeName).getUsedNamespace());
  }

  @Test
  public void testSetBucketQuotaIllegal() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    store.getVolume(volumeName).setQuota(OzoneQuota.parseQuota(
        "10GB", "1000"));
    store.getVolume(volumeName).createBucket(bucketName);

    // test bucket set quota 0
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid values for space quota",
        () -> store.getVolume(volumeName).getBucket(bucketName).setQuota(
            OzoneQuota.parseQuota("0GB", "10")));
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid values for namespace quota",
        () -> store.getVolume(volumeName).getBucket(bucketName).setQuota(
            OzoneQuota.parseQuota("10GB", "0")));

    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid values for namespace quota",
        () -> store.getVolume(volumeName).getBucket(bucketName).setQuota(
            OzoneQuota.parseQuota("1GB", "-100")));

    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid values for quota",
        () -> store.getVolume(volumeName).getBucket(bucketName).setQuota(
            OzoneQuota.parseQuota("1TEST", "100")));

    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid values for quota",
        () -> store.getVolume(volumeName).getBucket(bucketName).setQuota(
            OzoneQuota.parseQuota("9223372036854775808 BYTES", "100")));

    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid values for space quota",
        () -> store.getVolume(volumeName).getBucket(bucketName).setQuota(
            OzoneQuota.parseQuota("-10GB", "100")));

  }

  @Test
  public void testSetVolumeQuota() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    Assert.assertEquals(OzoneConsts.QUOTA_RESET,
        store.getVolume(volumeName).getQuotaInBytes());
    Assert.assertEquals(OzoneConsts.QUOTA_RESET,
        store.getVolume(volumeName).getQuotaInNamespace());
    store.getVolume(volumeName).setQuota(OzoneQuota.parseQuota("1GB", "1000"));
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertEquals(1024 * 1024 * 1024,
        volume.getQuotaInBytes());
    Assert.assertEquals(1000L, volume.getQuotaInNamespace());
  }

  @Test
  public void testSetVolumeQuotaIllegal() throws Exception {
    String volumeName = UUID.randomUUID().toString();

    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .addMetadata("key1", "val1")
        .setQuotaInNamespace(0)
        .setQuotaInBytes(0)
        .build();
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid values for quota",
        () -> store.createVolume(volumeName, volumeArgs));

    store.createVolume(volumeName);

    // test volume set quota 0
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid values for space quota",
        () -> store.getVolume(volumeName).setQuota(OzoneQuota.parseQuota(
            "0GB", "10")));
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid values for namespace quota",
        () -> store.getVolume(volumeName).setQuota(OzoneQuota.parseQuota(
            "10GB", "0")));

    // The unit should be legal.
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid values for quota",
        () -> store.getVolume(volumeName).setQuota(OzoneQuota.parseQuota(
            "1TEST", "1000")));

    // The setting value cannot be greater than LONG.MAX_VALUE BYTES.
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid values for quota",
        () -> store.getVolume(volumeName).setQuota(OzoneQuota.parseQuota(
            "9223372036854775808 B", "1000")));

    // The value cannot be negative.
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Invalid values for space quota",
        () -> store.getVolume(volumeName).setQuota(OzoneQuota.parseQuota(
            "-10GB", "1000")));
  }

  @Test
  public void testDeleteVolume()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertNotNull(volume);
    store.deleteVolume(volumeName);
    OzoneTestUtils.expectOmException(ResultCodes.VOLUME_NOT_FOUND,
        () -> store.getVolume(volumeName));

  }

  @Test
  public void testCreateVolumeWithMetadata()
      throws IOException, OzoneClientException {
    String volumeName = UUID.randomUUID().toString();
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .addMetadata("key1", "val1")
        .build();
    store.createVolume(volumeName, volumeArgs);
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertEquals(OzoneConsts.QUOTA_RESET, volume.getQuotaInNamespace());
    Assert.assertEquals(OzoneConsts.QUOTA_RESET, volume.getQuotaInBytes());
    Assert.assertEquals("val1", volume.getMetadata().get("key1"));
    Assert.assertEquals(volumeName, volume.getName());
  }

  @Test
  public void testCreateBucketWithMetadata()
      throws IOException, OzoneClientException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertEquals(OzoneConsts.QUOTA_RESET, volume.getQuotaInNamespace());
    Assert.assertEquals(OzoneConsts.QUOTA_RESET, volume.getQuotaInBytes());
    BucketArgs args = BucketArgs.newBuilder()
        .addMetadata("key1", "value1").build();
    volume.createBucket(bucketName, args);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertEquals(OzoneConsts.QUOTA_RESET, bucket.getQuotaInNamespace());
    Assert.assertEquals(OzoneConsts.QUOTA_RESET, bucket.getQuotaInBytes());
    Assert.assertNotNull(bucket.getMetadata());
    Assert.assertEquals("value1", bucket.getMetadata().get("key1"));

  }


  @Test
  public void testCreateBucket()
      throws IOException {
    Instant testStartTime = Instant.now();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertFalse(bucket.getCreationTime().isBefore(testStartTime));
    Assert.assertFalse(volume.getCreationTime().isBefore(testStartTime));
  }

  @Test
  public void testCreateS3Bucket()
      throws IOException {
    Instant testStartTime = Instant.now();
    String bucketName = UUID.randomUUID().toString();
    store.createS3Bucket(bucketName);
    OzoneBucket bucket = store.getS3Bucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertFalse(bucket.getCreationTime().isBefore(testStartTime));
  }

  @Test
  public void testDeleteS3Bucket()
      throws Exception {
    Instant testStartTime = Instant.now();
    String bucketName = UUID.randomUUID().toString();
    store.createS3Bucket(bucketName);
    OzoneBucket bucket = store.getS3Bucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertFalse(bucket.getCreationTime().isBefore(testStartTime));
    store.deleteS3Bucket(bucketName);

    OzoneTestUtils.expectOmException(ResultCodes.BUCKET_NOT_FOUND,
        () -> store.getS3Bucket(bucketName));
  }

  @Test
  public void testDeleteS3NonExistingBucket() {
    try {
      store.deleteS3Bucket(UUID.randomUUID().toString());
    } catch (IOException ex) {
      GenericTestUtils.assertExceptionContains("NOT_FOUND", ex);
    }
  }

  @Test
  public void testCreateBucketWithVersioning()
      throws IOException, OzoneClientException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setVersioning(true);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertEquals(true, bucket.getVersioning());
  }

  @Test
  public void testCreateBucketWithStorageType()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.SSD);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertEquals(StorageType.SSD, bucket.getStorageType());
  }

  @Test
  public void testCreateBucketWithAcls()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = new OzoneAcl(USER, "test",
        READ, ACCESS);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(userAcl);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setAcls(acls);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertTrue(bucket.getAcls().contains(userAcl));
  }

  @Test
  public void testCreateBucketWithAllArgument()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = new OzoneAcl(USER, "test",
        ACLType.ALL, ACCESS);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(userAcl);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setVersioning(true)
        .setStorageType(StorageType.SSD)
        .setAcls(acls);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertEquals(true, bucket.getVersioning());
    Assert.assertEquals(StorageType.SSD, bucket.getStorageType());
    Assert.assertTrue(bucket.getAcls().contains(userAcl));
  }

  @Test
  public void testInvalidBucketCreation() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = "invalid#bucket";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    LambdaTestUtils.intercept(OMException.class,
        "Bucket or Volume name has an unsupported" +
            " character : #",
        () -> volume.createBucket(bucketName));

  }

  @Test
  public void testAddBucketAcl()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(new OzoneAcl(USER, "test", ACLType.ALL, ACCESS));
    OzoneBucket bucket = volume.getBucket(bucketName);
    for (OzoneAcl acl : acls) {
      assertTrue(bucket.addAcls(acl));
    }
    OzoneBucket newBucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, newBucket.getName());
    Assert.assertTrue(bucket.getAcls().contains(acls.get(0)));
  }

  @Test
  public void testRemoveBucketAcl()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = new OzoneAcl(USER, "test",
        ACLType.ALL, ACCESS);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(userAcl);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setAcls(acls);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    for (OzoneAcl acl : acls) {
      assertTrue(bucket.removeAcls(acl));
    }
    OzoneBucket newBucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, newBucket.getName());
    Assert.assertTrue(!bucket.getAcls().contains(acls.get(0)));
  }

  @Test
  public void testRemoveBucketAclUsingRpcClientRemoveAcl()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = new OzoneAcl(USER, "test",
        ACLType.ALL, ACCESS);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(userAcl);
    acls.add(new OzoneAcl(USER, "test1",
        ACLType.ALL, ACCESS));
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setAcls(acls);
    volume.createBucket(bucketName, builder.build());
    OzoneObj ozoneObj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setResType(OzoneObj.ResourceType.BUCKET).build();

    // Remove the 2nd acl added to the list.
    boolean remove = store.removeAcl(ozoneObj, acls.get(1));
    Assert.assertTrue(remove);
    Assert.assertFalse(store.getAcl(ozoneObj).contains(acls.get(1)));

    remove = store.removeAcl(ozoneObj, acls.get(0));
    Assert.assertTrue(remove);
    Assert.assertFalse(store.getAcl(ozoneObj).contains(acls.get(0)));
  }

  @Test
  public void testSetBucketVersioning()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    bucket.setVersioning(true);
    OzoneBucket newBucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, newBucket.getName());
    Assert.assertEquals(true, newBucket.getVersioning());
  }

  @Test
  public void testAclsAfterCallingSetBucketProperty() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);

    OzoneBucket ozoneBucket = volume.getBucket(bucketName);
    List<OzoneAcl> currentAcls = ozoneBucket.getAcls();

    ozoneBucket.setVersioning(true);

    OzoneBucket newBucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, newBucket.getName());
    Assert.assertEquals(true, newBucket.getVersioning());

    List<OzoneAcl> aclsAfterSet = newBucket.getAcls();
    Assert.assertEquals(currentAcls, aclsAfterSet);

  }

  @Test
  public void testSetBucketStorageType()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    bucket.setStorageType(StorageType.SSD);
    OzoneBucket newBucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, newBucket.getName());
    Assert.assertEquals(StorageType.SSD, newBucket.getStorageType());
  }


  @Test
  public void testDeleteBucket()
      throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertNotNull(bucket);
    volume.deleteBucket(bucketName);

    OzoneTestUtils.expectOmException(ResultCodes.BUCKET_NOT_FOUND,
        () -> volume.getBucket(bucketName)
    );
  }

  private boolean verifyRatisReplication(String volumeName, String bucketName,
      String keyName, ReplicationType type, ReplicationFactor factor)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setRefreshPipeline(true)
        .build();
    HddsProtos.ReplicationType replicationType =
        HddsProtos.ReplicationType.valueOf(type.toString());
    HddsProtos.ReplicationFactor replicationFactor =
        HddsProtos.ReplicationFactor.valueOf(factor.getValue());
    OmKeyInfo keyInfo = ozoneManager.lookupKey(keyArgs);
    for (OmKeyLocationInfo info:
        keyInfo.getLatestVersionLocations().getLocationList()) {
      ContainerInfo container =
          storageContainerLocationClient.getContainer(info.getContainerID());
      if (!container.getReplicationFactor().equals(replicationFactor) || (
          container.getReplicationType() != replicationType)) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void testPutKey() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    Instant testStartTime = Instant.now();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes(UTF_8).length, STAND_ALONE,
          ONE, new HashMap<>());
      out.write(value.getBytes(UTF_8));
      out.close();
      OzoneKey key = bucket.getKey(keyName);
      Assert.assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes(UTF_8).length];
      is.read(fileContent);
      Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
          keyName, STAND_ALONE,
          ONE));
      Assert.assertEquals(value, new String(fileContent, UTF_8));
      Assert.assertFalse(key.getCreationTime().isBefore(testStartTime));
      Assert.assertFalse(key.getModificationTime().isBefore(testStartTime));
    }
  }

  @Test
  public void testCheckUsedBytesQuota() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneVolume volume = null;

    String value = "sample value";
    int blockSize = (int) ozoneManager.getConfiguration().getStorageSize(
        OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);
    int valueLength = value.getBytes(UTF_8).length;
    int countException = 0;

    store.createVolume(volumeName);
    volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    // Test bucket quota.
    store.getVolume(volumeName).setQuota(
        OzoneQuota.parseQuota(Long.MAX_VALUE + " B", "100"));
    bucketName = UUID.randomUUID().toString();
    volume.createBucket(bucketName);
    bucket = volume.getBucket(bucketName);
    bucket.setQuota(OzoneQuota.parseQuota("1 B", "100"));

    // Test bucket quota: write key.
    // The remaining quota does not satisfy a block size, so the write fails.
    try {
      writeKey(bucket, UUID.randomUUID().toString(), ONE, value, valueLength);
    } catch (IOException ex) {
      countException++;
      GenericTestUtils.assertExceptionContains("QUOTA_EXCEEDED", ex);
    }
    // Write failed, bucket usedBytes should be 0
    Assert.assertEquals(0L,
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());

    // Test bucket quota: write file.
    // The remaining quota does not satisfy a block size, so the write fails.
    try {
      writeFile(bucket, UUID.randomUUID().toString(), ONE, value, 0);
    } catch (IOException ex) {
      countException++;
      GenericTestUtils.assertExceptionContains("QUOTA_EXCEEDED", ex);
    }
    // Write failed, bucket usedBytes should be 0
    Assert.assertEquals(0L,
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());

    // Test bucket quota: write large key(with five blocks), the first four
    // blocks will succeed，while the later block will fail.
    bucket.setQuota(OzoneQuota.parseQuota(
        4 * blockSize + " B", "100"));

    try {
      OzoneOutputStream out = bucket.createKey(UUID.randomUUID().toString(),
          valueLength, STAND_ALONE, ONE, new HashMap<>());
      for (int i = 0; i <= (4 * blockSize) / value.length(); i++) {
        out.write(value.getBytes(UTF_8));
      }
      out.close();
    } catch (IOException ex) {
      countException++;
      GenericTestUtils.assertExceptionContains("QUOTA_EXCEEDED", ex);
    }
    // AllocateBlock failed, bucket usedBytes should be 4 * blockSize
    Assert.assertEquals(4 * blockSize,
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());

    // Reset bucket quota, the original usedBytes needs to remain the same
    bucket.setQuota(OzoneQuota.parseQuota(
        100 + " GB", "100"));
    Assert.assertEquals(4 * blockSize,
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());

    Assert.assertEquals(3, countException);

    // key with 0 bytes, usedBytes should not increase.
    bucket.setQuota(OzoneQuota.parseQuota(
        5 * blockSize + " B", "100"));
    OzoneOutputStream out = bucket.createKey(UUID.randomUUID().toString(),
        valueLength, STAND_ALONE, ONE, new HashMap<>());
    out.close();
    Assert.assertEquals(4 * blockSize,
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());
  }

  @Test
  public void testVolumeUsedNamespace() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String bucketName2 = UUID.randomUUID().toString();
    OzoneVolume volume = null;

    // set Volume namespace quota as 1
    store.createVolume(volumeName,
        VolumeArgs.newBuilder().setQuotaInNamespace(1L).build());
    volume = store.getVolume(volumeName);
    // The initial value should be 0
    Assert.assertEquals(0L, volume.getUsedNamespace());
    volume.createBucket(bucketName);
    // Used namespace should be 1
    volume = store.getVolume(volumeName);
    Assert.assertEquals(1L, volume.getUsedNamespace());

    try {
      volume.createBucket(bucketName2);
    } catch (IOException ex) {
      GenericTestUtils.assertExceptionContains("QUOTA_EXCEEDED", ex);
    }

    // test linked bucket
    String targetVolName = UUID.randomUUID().toString();
    store.createVolume(targetVolName);
    OzoneVolume volumeWithLinkedBucket = store.getVolume(targetVolName);
    String targetBucketName = UUID.randomUUID().toString();
    BucketArgs.Builder argsBuilder = new BucketArgs.Builder()
        .setStorageType(StorageType.DEFAULT)
        .setVersioning(false)
        .setSourceVolume(volumeName)
        .setSourceBucket(bucketName);
    volumeWithLinkedBucket.createBucket(targetBucketName, argsBuilder.build());
    // Used namespace should be 0 because linked bucket does not consume
    // namespace quota
    Assert.assertEquals(0L, volumeWithLinkedBucket.getUsedNamespace());

    // Reset volume quota, the original usedNamespace needs to remain the same
    store.getVolume(volumeName).setQuota(OzoneQuota.parseQuota(
        100 + " GB", "100"));
    Assert.assertEquals(1L,
        store.getVolume(volumeName).getUsedNamespace());

    volume.deleteBucket(bucketName);
    // Used namespace should be 0
    volume = store.getVolume(volumeName);
    Assert.assertEquals(0L, volume.getUsedNamespace());
  }

  @Test
  public void testBucketUsedNamespace() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String key1 = UUID.randomUUID().toString();
    String key2 = UUID.randomUUID().toString();
    String key3 = UUID.randomUUID().toString();
    OzoneVolume volume = null;
    OzoneBucket bucket = null;

    String value = "sample value";

    store.createVolume(volumeName);
    volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    bucket = volume.getBucket(bucketName);
    bucket.setQuota(OzoneQuota.parseQuota(Long.MAX_VALUE + " B", "2"));

    writeKey(bucket, key1, ONE, value, value.length());
    Assert.assertEquals(1L,
        store.getVolume(volumeName).getBucket(bucketName).getUsedNamespace());

    writeKey(bucket, key2, ONE, value, value.length());
    Assert.assertEquals(2L,
        store.getVolume(volumeName).getBucket(bucketName).getUsedNamespace());

    try {
      writeKey(bucket, key3, ONE, value, value.length());
      Assert.fail("Write key should be failed");
    } catch (IOException ex) {
      GenericTestUtils.assertExceptionContains("QUOTA_EXCEEDED", ex);
    }

    // Write failed, bucket usedNamespace should remain as 2
    Assert.assertEquals(2L,
        store.getVolume(volumeName).getBucket(bucketName).getUsedNamespace());

    // Reset bucket quota, the original usedNamespace needs to remain the same
    bucket.setQuota(
        OzoneQuota.parseQuota(Long.MAX_VALUE + " B", "100"));
    Assert.assertEquals(2L,
        store.getVolume(volumeName).getBucket(bucketName).getUsedNamespace());

    bucket.deleteKeys(Arrays.asList(key1, key2));
    Assert.assertEquals(0L,
        store.getVolume(volumeName).getBucket(bucketName).getUsedNamespace());
  }

  private void writeKey(OzoneBucket bucket, String keyName,
      ReplicationFactor replication, String value, int valueLength)
      throws IOException{
    OzoneOutputStream out = bucket.createKey(keyName, valueLength, STAND_ALONE,
        replication, new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();
  }

  private void writeFile(OzoneBucket bucket, String keyName,
      ReplicationFactor replication, String value, int valueLength)
      throws IOException{
    OzoneOutputStream out = bucket.createFile(keyName, valueLength, STAND_ALONE,
        replication, true, true);
    out.write(value.getBytes(UTF_8));
    out.close();
  }

  @Test
  public void testUsedBytesWithUploadPart() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    int blockSize = (int) ozoneManager.getConfiguration().getStorageSize(
        OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);
    String sampleData = Arrays.toString(generateData(blockSize + 100,
        (byte) RandomUtils.nextLong()));
    int valueLength = sampleData.getBytes(UTF_8).length;

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        STAND_ALONE, ONE);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    assertNotNull(multipartInfo.getUploadID());

    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), 1, uploadID);
    ozoneOutputStream.write(string2Bytes(sampleData), 0,
        sampleData.length());
    ozoneOutputStream.close();

    Assert.assertEquals(valueLength, store.getVolume(volumeName)
        .getBucket(bucketName).getUsedBytes());

    // Abort uploaded partKey and the usedBytes of bucket should be 0.
    bucket.abortMultipartUpload(keyName, uploadID);
    Assert.assertEquals(0, store.getVolume(volumeName)
        .getBucket(bucketName).getUsedBytes());
  }

  @Test
  public void testValidateBlockLengthWithCommitKey() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = RandomStringUtils.random(RandomUtils.nextInt(0, 1024));
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();

    // create the initial key with size 0, write will allocate the first block.
    OzoneOutputStream out = bucket.createKey(keyName, 0,
        STAND_ALONE, ONE, new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();
    OmKeyArgs.Builder builder = new OmKeyArgs.Builder();
    builder.setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName).setRefreshPipeline(true);
    OmKeyInfo keyInfo = ozoneManager.lookupKey(builder.build());

    List<OmKeyLocationInfo> locationInfoList =
        keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();
    // LocationList should have only 1 block
    Assert.assertEquals(1, locationInfoList.size());
    // make sure the data block size is updated
    Assert.assertEquals(value.getBytes(UTF_8).length,
        locationInfoList.get(0).getLength());
    // make sure the total data size is set correctly
    Assert.assertEquals(value.getBytes(UTF_8).length, keyInfo.getDataSize());
  }

  @Test
  public void testPutKeyRatisOneNode() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    Instant testStartTime = Instant.now();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes(UTF_8).length, ReplicationType.RATIS,
          ONE, new HashMap<>());
      out.write(value.getBytes(UTF_8));
      out.close();
      OzoneKey key = bucket.getKey(keyName);
      Assert.assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes(UTF_8).length];
      is.read(fileContent);
      is.close();
      Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
          keyName, ReplicationType.RATIS, ONE));
      Assert.assertEquals(value, new String(fileContent, UTF_8));
      Assert.assertFalse(key.getCreationTime().isBefore(testStartTime));
      Assert.assertFalse(key.getModificationTime().isBefore(testStartTime));
    }
  }

  @Test
  public void testPutKeyRatisThreeNodes() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    Instant testStartTime = Instant.now();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes(UTF_8).length, ReplicationType.RATIS,
          THREE, new HashMap<>());
      out.write(value.getBytes(UTF_8));
      out.close();
      OzoneKey key = bucket.getKey(keyName);
      Assert.assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes(UTF_8).length];
      is.read(fileContent);
      is.close();
      Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
          keyName, ReplicationType.RATIS,
          THREE));
      Assert.assertEquals(value, new String(fileContent, UTF_8));
      Assert.assertFalse(key.getCreationTime().isBefore(testStartTime));
      Assert.assertFalse(key.getModificationTime().isBefore(testStartTime));
    }
  }


  @Test
  public void testPutKeyRatisThreeNodesParallel() throws IOException,
      InterruptedException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    Instant testStartTime = Instant.now();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    CountDownLatch latch = new CountDownLatch(2);
    AtomicInteger failCount = new AtomicInteger(0);

    Runnable r = () -> {
      try {
        for (int i = 0; i < 5; i++) {
          String keyName = UUID.randomUUID().toString();
          String data = Arrays.toString(generateData(5 * 1024 * 1024,
              (byte) RandomUtils.nextLong()));
          OzoneOutputStream out = bucket.createKey(keyName,
              data.getBytes(UTF_8).length, ReplicationType.RATIS,
              THREE, new HashMap<>());
          out.write(data.getBytes(UTF_8));
          out.close();
          OzoneKey key = bucket.getKey(keyName);
          Assert.assertEquals(keyName, key.getName());
          OzoneInputStream is = bucket.readKey(keyName);
          byte[] fileContent = new byte[data.getBytes(UTF_8).length];
          is.read(fileContent);
          is.close();
          Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
              keyName, ReplicationType.RATIS,
              THREE));
          Assert.assertEquals(data, new String(fileContent, UTF_8));
          Assert.assertFalse(key.getCreationTime().isBefore(testStartTime));
          Assert.assertFalse(key.getModificationTime().isBefore(testStartTime));
        }
        latch.countDown();
      } catch (IOException ex) {
        latch.countDown();
        failCount.incrementAndGet();
      }
    };

    Thread thread1 = new Thread(r);
    Thread thread2 = new Thread(r);

    thread1.start();
    thread2.start();

    latch.await(600, TimeUnit.SECONDS);

    if (failCount.get() > 0) {
      fail("testPutKeyRatisThreeNodesParallel failed");
    }

  }


  @Test
  public void testReadKeyWithVerifyChecksumFlagEnable() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    // Create and corrupt key
    createAndCorruptKey(volumeName, bucketName, keyName);

    // read corrupt key with verify checksum enabled
    readCorruptedKey(volumeName, bucketName, keyName, true);

  }


  @Test
  public void testReadKeyWithVerifyChecksumFlagDisable() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    // Create and corrupt key
    createAndCorruptKey(volumeName, bucketName, keyName);

    // read corrupt key with verify checksum enabled
    readCorruptedKey(volumeName, bucketName, keyName, false);

  }

  private void createAndCorruptKey(String volumeName, String bucketName,
      String keyName) throws IOException {
    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    // Write data into a key
    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(UTF_8).length, ReplicationType.RATIS,
        ONE, new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();

    // We need to find the location of the chunk file corresponding to the
    // data we just wrote.
    OzoneKey key = bucket.getKey(keyName);
    long containerID = ((OzoneKeyDetails) key).getOzoneKeyLocations().get(0)
        .getContainerID();

    // Get the container by traversing the datanodes. Atleast one of the
    // datanode must have this container.
    Container container = null;
    for (HddsDatanodeService hddsDatanode : cluster.getHddsDatanodes()) {
      container = hddsDatanode.getDatanodeStateMachine().getContainer()
          .getContainerSet().getContainer(containerID);
      if (container != null) {
        break;
      }
    }
    Assert.assertNotNull("Container not found", container);
    corruptData(container, key);
  }


  private void readCorruptedKey(String volumeName, String bucketName,
      String keyName, boolean verifyChecksum) {
    try {

      OzoneConfiguration configuration = cluster.getConf();

      final OzoneClientConfig clientConfig =
          configuration.getObject(OzoneClientConfig.class);
      clientConfig.setChecksumVerify(verifyChecksum);
      configuration.setFromObject(clientConfig);

      RpcClient client = new RpcClient(configuration, null);
      OzoneInputStream is = client.getKey(volumeName, bucketName, keyName);
      is.read(new byte[100]);
      is.close();
      if (verifyChecksum) {
        fail("Reading corrupted data should fail, as verify checksum is " +
            "enabled");
      }
    } catch (IOException e) {
      if (!verifyChecksum) {
        fail("Reading corrupted data should not fail, as verify checksum is " +
            "disabled");
      }
    }
  }


  private void readKey(OzoneBucket bucket, String keyName, String data)
      throws IOException {
    OzoneKey key = bucket.getKey(keyName);
    Assert.assertEquals(keyName, key.getName());
    OzoneInputStream is = bucket.readKey(keyName);
    byte[] fileContent = new byte[data.getBytes(UTF_8).length];
    is.read(fileContent);
    is.close();
  }

  @Test
  public void testGetKeyDetails() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();
    String keyValue = RandomStringUtils.random(128);
    //String keyValue = "this is a test value.glx";
    // create the initial key with size 0, write will allocate the first block.
    OzoneOutputStream out = bucket.createKey(keyName,
        keyValue.getBytes(UTF_8).length, STAND_ALONE,
        ONE, new HashMap<>());
    out.write(keyValue.getBytes(UTF_8));
    out.close();

    OzoneInputStream is = bucket.readKey(keyName);
    byte[] fileContent = new byte[32];
    is.read(fileContent);

    // First, confirm the key info from the client matches the info in OM.
    OmKeyArgs.Builder builder = new OmKeyArgs.Builder();
    builder.setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName).setRefreshPipeline(true);
    OmKeyLocationInfo keyInfo = ozoneManager.lookupKey(builder.build()).
        getKeyLocationVersions().get(0).getBlocksLatestVersionOnly().get(0);
    long containerID = keyInfo.getContainerID();
    long localID = keyInfo.getLocalID();
    OzoneKeyDetails keyDetails = (OzoneKeyDetails)bucket.getKey(keyName);
    Assert.assertEquals(keyName, keyDetails.getName());

    List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();
    Assert.assertEquals(1, keyLocations.size());
    Assert.assertEquals(containerID, keyLocations.get(0).getContainerID());
    Assert.assertEquals(localID, keyLocations.get(0).getLocalID());

    // Make sure that the data size matched.
    Assert.assertEquals(keyValue.getBytes(UTF_8).length,
        keyLocations.get(0).getLength());

    // Second, sum the data size from chunks in Container via containerID
    // and localID, make sure the size equals to the size from keyDetails.
    ContainerInfo container = cluster.getStorageContainerManager()
        .getContainerManager().getContainer(ContainerID.valueOf(containerID));
    Pipeline pipeline = cluster.getStorageContainerManager()
        .getPipelineManager().getPipeline(container.getPipelineID());
    List<DatanodeDetails> datanodes = pipeline.getNodes();
    Assert.assertEquals(datanodes.size(), 1);

    DatanodeDetails datanodeDetails = datanodes.get(0);
    Assert.assertNotNull(datanodeDetails);
    HddsDatanodeService datanodeService = null;
    for (HddsDatanodeService datanodeServiceItr : cluster.getHddsDatanodes()) {
      if (datanodeDetails.equals(datanodeServiceItr.getDatanodeDetails())) {
        datanodeService = datanodeServiceItr;
        break;
      }
    }
    KeyValueContainerData containerData =
        (KeyValueContainerData)(datanodeService.getDatanodeStateMachine()
            .getContainer().getContainerSet().getContainer(containerID)
            .getContainerData());
    try (ReferenceCountedDB db = BlockUtils.getDB(containerData,
            cluster.getConf());
         BlockIterator<BlockData> keyValueBlockIterator =
                db.getStore().getBlockIterator()) {
      while (keyValueBlockIterator.hasNext()) {
        BlockData blockData = keyValueBlockIterator.nextBlock();
        if (blockData.getBlockID().getLocalID() == localID) {
          long length = 0;
          List<ContainerProtos.ChunkInfo> chunks = blockData.getChunks();
          for (ContainerProtos.ChunkInfo chunk : chunks) {
            length += chunk.getLen();
          }
          Assert.assertEquals(length, keyValue.getBytes(UTF_8).length);
          break;
        }
      }
    }
  }

  /**
   * Tests reading a corrputed chunk file throws checksum exception.
   * @throws IOException
   */
  @Test
  public void testReadKeyWithCorruptedData() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();

    // Write data into a key
    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(UTF_8).length, ReplicationType.RATIS,
        ONE, new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();

    // We need to find the location of the chunk file corresponding to the
    // data we just wrote.
    OzoneKey key = bucket.getKey(keyName);
    long containerID = ((OzoneKeyDetails) key).getOzoneKeyLocations().get(0)
        .getContainerID();

    // Get the container by traversing the datanodes. Atleast one of the
    // datanode must have this container.
    Container container = null;
    for (HddsDatanodeService hddsDatanode : cluster.getHddsDatanodes()) {
      container = hddsDatanode.getDatanodeStateMachine().getContainer()
          .getContainerSet().getContainer(containerID);
      if (container != null) {
        break;
      }
    }
    Assert.assertNotNull("Container not found", container);
    corruptData(container, key);

    // Try reading the key. Since the chunk file is corrupted, it should
    // throw a checksum mismatch exception.
    try {
      OzoneInputStream is = bucket.readKey(keyName);
      is.read(new byte[100]);
      fail("Reading corrupted data should fail.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Checksum mismatch", e);
    }
  }

  /**
   * Tests reading a corrputed chunk file throws checksum exception.
   * @throws IOException
   */
  @Test
  public void testReadKeyWithCorruptedDataWithMutiNodes() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = "sample value";
    byte[] data = value.getBytes(UTF_8);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();

    // Write data into a key
    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(UTF_8).length, ReplicationType.RATIS,
        THREE, new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();

    // We need to find the location of the chunk file corresponding to the
    // data we just wrote.
    OzoneKey key = bucket.getKey(keyName);
    List<OzoneKeyLocation> keyLocation =
        ((OzoneKeyDetails) key).getOzoneKeyLocations();
    Assert.assertTrue("Key location not found in OM", !keyLocation.isEmpty());
    long containerID = ((OzoneKeyDetails) key).getOzoneKeyLocations().get(0)
        .getContainerID();

    // Get the container by traversing the datanodes.
    List<Container> containerList = new ArrayList<>();
    Container container;
    for (HddsDatanodeService hddsDatanode : cluster.getHddsDatanodes()) {
      container = hddsDatanode.getDatanodeStateMachine().getContainer()
          .getContainerSet().getContainer(containerID);
      if (container != null) {
        containerList.add(container);
        if (containerList.size() == 3) {
          break;
        }
      }
    }
    Assert.assertTrue("Container not found", !containerList.isEmpty());
    corruptData(containerList.get(0), key);
    // Try reading the key. Read will fail on the first node and will eventually
    // failover to next replica
    try {
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] b = new byte[data.length];
      is.read(b);
      Assert.assertTrue(Arrays.equals(b, data));
    } catch (OzoneChecksumException e) {
      fail("Reading corrupted data should not fail.");
    }
    corruptData(containerList.get(1), key);
    // Try reading the key. Read will fail on the first node and will eventually
    // failover to next replica
    try {
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] b = new byte[data.length];
      is.read(b);
      Assert.assertTrue(Arrays.equals(b, data));
    } catch (OzoneChecksumException e) {
      fail("Reading corrupted data should not fail.");
    }
    corruptData(containerList.get(2), key);
    // Try reading the key. Read will fail here as all the replica are corrupt
    try {
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] b = new byte[data.length];
      is.read(b);
      fail("Reading corrupted data should fail.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Checksum mismatch", e);
    }
  }

  private void corruptData(Container container, OzoneKey key)
      throws IOException {
    long containerID = ((OzoneKeyDetails) key).getOzoneKeyLocations().get(0)
        .getContainerID();
    long localID = ((OzoneKeyDetails) key).getOzoneKeyLocations().get(0)
        .getLocalID();
    // From the containerData, get the block iterator for all the blocks in
    // the container.
    KeyValueContainerData containerData =
        (KeyValueContainerData) container.getContainerData();
    try (ReferenceCountedDB db = BlockUtils.getDB(containerData,
            cluster.getConf());
         BlockIterator<BlockData> keyValueBlockIterator =
                 db.getStore().getBlockIterator()) {
      // Find the block corresponding to the key we put. We use the localID of
      // the BlockData to identify out key.
      BlockData blockData = null;
      while (keyValueBlockIterator.hasNext()) {
        blockData = keyValueBlockIterator.nextBlock();
        if (blockData.getBlockID().getLocalID() == localID) {
          break;
        }
      }
      Assert.assertNotNull("Block not found", blockData);

      // Get the location of the chunk file
      String containreBaseDir =
          container.getContainerData().getVolume().getHddsRootDir().getPath();
      File chunksLocationPath = KeyValueContainerLocationUtil
          .getChunksLocationPath(containreBaseDir, clusterId, containerID);
      byte[] corruptData = "corrupted data".getBytes(UTF_8);
      // Corrupt the contents of chunk files
      for (File file : FileUtils.listFiles(chunksLocationPath, null, false)) {
        FileUtils.writeByteArrayToFile(file, corruptData);
      }
    }
  }

  @Test
  public void testDeleteKey()
      throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(UTF_8).length, STAND_ALONE,
        ONE, new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();
    OzoneKey key = bucket.getKey(keyName);
    Assert.assertEquals(keyName, key.getName());
    bucket.deleteKey(keyName);

    OzoneTestUtils.expectOmException(KEY_NOT_FOUND,
        () -> bucket.getKey(keyName));
  }

  @Test
  public void testRenameKey()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String fromKeyName = UUID.randomUUID().toString();
    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    createTestKey(bucket, fromKeyName, value);

    // Rename to empty string should fail.
    OMException oe = null;
    String toKeyName = "";
    try {
      bucket.renameKey(fromKeyName, toKeyName);
    } catch (OMException e) {
      oe = e;
    }
    Assert.assertEquals(ResultCodes.INVALID_KEY_NAME, oe.getResult());

    toKeyName = UUID.randomUUID().toString();
    bucket.renameKey(fromKeyName, toKeyName);

    // Lookup for old key should fail.
    try {
      bucket.getKey(fromKeyName);
    } catch (OMException e) {
      oe = e;
    }
    Assert.assertEquals(KEY_NOT_FOUND, oe.getResult());

    OzoneKey key = bucket.getKey(toKeyName);
    Assert.assertEquals(toKeyName, key.getName());
  }

  @Test
  public void testKeysRename() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName1 = "dir/file1";
    String keyName2 = "dir/file2";

    String newKeyName1 = "dir/key1";
    String newKeyName2 = "dir/key2";

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    createTestKey(bucket, keyName1, value);
    createTestKey(bucket, keyName2, value);

    Map<String, String> keyMap = new HashMap();
    keyMap.put(keyName1, newKeyName1);
    keyMap.put(keyName2, newKeyName2);
    bucket.renameKeys(keyMap);

    // new key should exist
    Assert.assertEquals(newKeyName1, bucket.getKey(newKeyName1).getName());
    Assert.assertEquals(newKeyName2, bucket.getKey(newKeyName2).getName());

    // old key should not exist
    assertKeyRenamedEx(bucket, keyName1);
    assertKeyRenamedEx(bucket, keyName2);
  }

  @Test
  public void testKeysRenameFail() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName1 = "dir/file1";
    String keyName2 = "dir/file2";

    String newKeyName1 = "dir/key1";
    String newKeyName2 = "dir/key2";

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    // Create only keyName1 to test the partial failure of renameKeys.
    createTestKey(bucket, keyName1, value);

    Map<String, String> keyMap = new HashMap();
    keyMap.put(keyName1, newKeyName1);
    keyMap.put(keyName2, newKeyName2);

    try {
      bucket.renameKeys(keyMap);
    } catch (OMException ex) {
      Assert.assertEquals(PARTIAL_RENAME, ex.getResult());
    }

    // newKeyName1 should exist
    Assert.assertEquals(newKeyName1, bucket.getKey(newKeyName1).getName());
    // newKeyName2 should not exist
    assertKeyRenamedEx(bucket, keyName2);
  }

  @Test
  public void testListVolume() throws IOException {
    String volBase = "vol-" + RandomStringUtils.randomNumeric(3);
    //Create 10 volume vol-<random>-a-0-<random> to vol-<random>-a-9-<random>
    String volBaseNameA = volBase + "-a-";
    for(int i = 0; i < 10; i++) {
      store.createVolume(
          volBaseNameA + i + "-" + RandomStringUtils.randomNumeric(5));
    }
    //Create 10 volume vol-<random>-b-0-<random> to vol-<random>-b-9-<random>
    String volBaseNameB = volBase + "-b-";
    for(int i = 0; i < 10; i++) {
      store.createVolume(
          volBaseNameB + i + "-" + RandomStringUtils.randomNumeric(5));
    }
    Iterator<? extends OzoneVolume> volIterator = store.listVolumes(volBase);
    int totalVolumeCount = 0;
    while(volIterator.hasNext()) {
      volIterator.next();
      totalVolumeCount++;
    }
    Assert.assertEquals(20, totalVolumeCount);
    Iterator<? extends OzoneVolume> volAIterator = store.listVolumes(
        volBaseNameA);
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volAIterator.next().getName()
          .startsWith(volBaseNameA + i + "-"));
    }
    Assert.assertFalse(volAIterator.hasNext());
    Iterator<? extends OzoneVolume> volBIterator = store.listVolumes(
        volBaseNameB);
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volBIterator.next().getName()
          .startsWith(volBaseNameB + i + "-"));
    }
    Assert.assertFalse(volBIterator.hasNext());
    Iterator<? extends OzoneVolume> iter = store.listVolumes(volBaseNameA +
        "1-");
    Assert.assertTrue(iter.next().getName().startsWith(volBaseNameA + "1-"));
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testListBucket()
      throws IOException {
    String volumeA = "vol-a-" + RandomStringUtils.randomNumeric(5);
    String volumeB = "vol-b-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volumeA);
    store.createVolume(volumeB);
    OzoneVolume volA = store.getVolume(volumeA);
    OzoneVolume volB = store.getVolume(volumeB);

    //Create 10 buckets in  vol-a-<random> and 10 in vol-b-<random>
    String bucketBaseNameA = "bucket-a-";
    for(int i = 0; i < 10; i++) {
      volA.createBucket(
          bucketBaseNameA + i + "-" + RandomStringUtils.randomNumeric(5));
      volB.createBucket(
          bucketBaseNameA + i + "-" + RandomStringUtils.randomNumeric(5));
    }
    //Create 10 buckets in vol-a-<random> and 10 in vol-b-<random>
    String bucketBaseNameB = "bucket-b-";
    for(int i = 0; i < 10; i++) {
      volA.createBucket(
          bucketBaseNameB + i + "-" + RandomStringUtils.randomNumeric(5));
      volB.createBucket(
          bucketBaseNameB + i + "-" + RandomStringUtils.randomNumeric(5));
    }
    Iterator<? extends OzoneBucket> volABucketIter =
        volA.listBuckets("bucket-");
    int volABucketCount = 0;
    while(volABucketIter.hasNext()) {
      volABucketIter.next();
      volABucketCount++;
    }
    Assert.assertEquals(20, volABucketCount);
    Iterator<? extends OzoneBucket> volBBucketIter =
        volA.listBuckets("bucket-");
    int volBBucketCount = 0;
    while(volBBucketIter.hasNext()) {
      volBBucketIter.next();
      volBBucketCount++;
    }
    Assert.assertEquals(20, volBBucketCount);

    Iterator<? extends OzoneBucket> volABucketAIter =
        volA.listBuckets("bucket-a-");
    int volABucketACount = 0;
    while(volABucketAIter.hasNext()) {
      volABucketAIter.next();
      volABucketACount++;
    }
    Assert.assertEquals(10, volABucketACount);
    Iterator<? extends OzoneBucket> volBBucketBIter =
        volA.listBuckets("bucket-b-");
    int volBBucketBCount = 0;
    while(volBBucketBIter.hasNext()) {
      volBBucketBIter.next();
      volBBucketBCount++;
    }
    Assert.assertEquals(10, volBBucketBCount);
    Iterator<? extends OzoneBucket> volABucketBIter = volA.listBuckets(
        "bucket-b-");
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volABucketBIter.next().getName()
          .startsWith(bucketBaseNameB + i + "-"));
    }
    Assert.assertFalse(volABucketBIter.hasNext());
    Iterator<? extends OzoneBucket> volBBucketAIter = volB.listBuckets(
        "bucket-a-");
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volBBucketAIter.next().getName()
          .startsWith(bucketBaseNameA + i + "-"));
    }
    Assert.assertFalse(volBBucketAIter.hasNext());

  }

  @Test
  public void testListBucketsOnEmptyVolume()
      throws IOException {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    Iterator<? extends OzoneBucket> buckets = vol.listBuckets("");
    while(buckets.hasNext()) {
      fail();
    }
  }

  @Test
  public void testListKey()
      throws IOException {
    String volumeA = "vol-a-" + RandomStringUtils.randomNumeric(5);
    String volumeB = "vol-b-" + RandomStringUtils.randomNumeric(5);
    String bucketA = "buc-a-" + RandomStringUtils.randomNumeric(5);
    String bucketB = "buc-b-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volumeA);
    store.createVolume(volumeB);
    OzoneVolume volA = store.getVolume(volumeA);
    OzoneVolume volB = store.getVolume(volumeB);
    volA.createBucket(bucketA);
    volA.createBucket(bucketB);
    volB.createBucket(bucketA);
    volB.createBucket(bucketB);
    OzoneBucket volAbucketA = volA.getBucket(bucketA);
    OzoneBucket volAbucketB = volA.getBucket(bucketB);
    OzoneBucket volBbucketA = volB.getBucket(bucketA);
    OzoneBucket volBbucketB = volB.getBucket(bucketB);

    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseA = "key-a-";
    for (int i = 0; i < 10; i++) {
      byte[] value = RandomStringUtils.randomAscii(10240).getBytes(UTF_8);
      OzoneOutputStream one = volAbucketA.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, STAND_ALONE, ONE,
          new HashMap<>());
      one.write(value);
      one.close();
      OzoneOutputStream two = volAbucketB.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, STAND_ALONE, ONE,
          new HashMap<>());
      two.write(value);
      two.close();
      OzoneOutputStream three = volBbucketA.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, STAND_ALONE, ONE,
          new HashMap<>());
      three.write(value);
      three.close();
      OzoneOutputStream four = volBbucketB.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, STAND_ALONE, ONE,
          new HashMap<>());
      four.write(value);
      four.close();
    }
    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseB = "key-b-";
    for (int i = 0; i < 10; i++) {
      byte[] value = RandomStringUtils.randomAscii(10240).getBytes(UTF_8);
      OzoneOutputStream one = volAbucketA.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, STAND_ALONE, ONE,
          new HashMap<>());
      one.write(value);
      one.close();
      OzoneOutputStream two = volAbucketB.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, STAND_ALONE, ONE,
          new HashMap<>());
      two.write(value);
      two.close();
      OzoneOutputStream three = volBbucketA.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, STAND_ALONE, ONE,
          new HashMap<>());
      three.write(value);
      three.close();
      OzoneOutputStream four = volBbucketB.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, STAND_ALONE, ONE,
          new HashMap<>());
      four.write(value);
      four.close();
    }
    Iterator<? extends OzoneKey> volABucketAIter =
        volAbucketA.listKeys("key-");
    int volABucketAKeyCount = 0;
    while(volABucketAIter.hasNext()) {
      volABucketAIter.next();
      volABucketAKeyCount++;
    }
    Assert.assertEquals(20, volABucketAKeyCount);
    Iterator<? extends OzoneKey> volABucketBIter =
        volAbucketB.listKeys("key-");
    int volABucketBKeyCount = 0;
    while(volABucketBIter.hasNext()) {
      volABucketBIter.next();
      volABucketBKeyCount++;
    }
    Assert.assertEquals(20, volABucketBKeyCount);
    Iterator<? extends OzoneKey> volBBucketAIter =
        volBbucketA.listKeys("key-");
    int volBBucketAKeyCount = 0;
    while(volBBucketAIter.hasNext()) {
      volBBucketAIter.next();
      volBBucketAKeyCount++;
    }
    Assert.assertEquals(20, volBBucketAKeyCount);
    Iterator<? extends OzoneKey> volBBucketBIter =
        volBbucketB.listKeys("key-");
    int volBBucketBKeyCount = 0;
    while(volBBucketBIter.hasNext()) {
      volBBucketBIter.next();
      volBBucketBKeyCount++;
    }
    Assert.assertEquals(20, volBBucketBKeyCount);
    Iterator<? extends OzoneKey> volABucketAKeyAIter =
        volAbucketA.listKeys("key-a-");
    int volABucketAKeyACount = 0;
    while(volABucketAKeyAIter.hasNext()) {
      volABucketAKeyAIter.next();
      volABucketAKeyACount++;
    }
    Assert.assertEquals(10, volABucketAKeyACount);
    Iterator<? extends OzoneKey> volABucketAKeyBIter =
        volAbucketA.listKeys("key-b-");
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volABucketAKeyBIter.next().getName()
          .startsWith("key-b-" + i + "-"));
    }
    Assert.assertFalse(volABucketBIter.hasNext());
  }

  @Test
  public void testListKeyOnEmptyBucket()
      throws IOException {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buc-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    vol.createBucket(bucket);
    OzoneBucket buc = vol.getBucket(bucket);
    Iterator<? extends OzoneKey> keys = buc.listKeys("");
    while(keys.hasNext()) {
      fail();
    }
  }

  @Test
  public void testInitiateMultipartUploadWithReplicationInformationSet() throws
      IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        STAND_ALONE, ONE);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    assertNotNull(multipartInfo.getUploadID());

    // Call initiate multipart upload for the same key again, this should
    // generate a new uploadID.
    multipartInfo = bucket.initiateMultipartUpload(keyName,
        STAND_ALONE, ONE);

    assertNotNull(multipartInfo);
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    assertNotEquals(multipartInfo.getUploadID(), uploadID);
    assertNotNull(multipartInfo.getUploadID());
  }


  @Test
  public void testInitiateMultipartUploadWithDefaultReplication() throws
      IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    assertNotNull(multipartInfo.getUploadID());

    // Call initiate multipart upload for the same key again, this should
    // generate a new uploadID.
    multipartInfo = bucket.initiateMultipartUpload(keyName);

    assertNotNull(multipartInfo);
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    assertNotEquals(multipartInfo.getUploadID(), uploadID);
    assertNotNull(multipartInfo.getUploadID());
  }

  @Test
  public void testUploadPartWithNoOverride() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String sampleData = "sample Value";

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        STAND_ALONE, ONE);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    assertNotNull(multipartInfo.getUploadID());

    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), 1, uploadID);
    ozoneOutputStream.write(string2Bytes(sampleData), 0, sampleData.length());
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo commitUploadPartInfo = ozoneOutputStream
        .getCommitUploadPartInfo();

    assertNotNull(commitUploadPartInfo);
    String partName = commitUploadPartInfo.getPartName();
    assertNotNull(commitUploadPartInfo.getPartName());

  }

  @Test
  public void testUploadPartOverrideWithStandAlone() throws IOException {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String sampleData = "sample Value";
    int partNumber = 1;

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        STAND_ALONE, ONE);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    assertNotNull(multipartInfo.getUploadID());

    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), partNumber, uploadID);
    ozoneOutputStream.write(string2Bytes(sampleData), 0, sampleData.length());
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo commitUploadPartInfo = ozoneOutputStream
        .getCommitUploadPartInfo();

    assertNotNull(commitUploadPartInfo);
    String partName = commitUploadPartInfo.getPartName();
    assertNotNull(commitUploadPartInfo.getPartName());

    //Overwrite the part by creating part key with same part number.
    sampleData = "sample Data Changed";
    ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), partNumber, uploadID);
    ozoneOutputStream.write(string2Bytes(sampleData), 0, "name".length());
    ozoneOutputStream.close();

    commitUploadPartInfo = ozoneOutputStream
        .getCommitUploadPartInfo();

    assertNotNull(commitUploadPartInfo);
    assertNotNull(commitUploadPartInfo.getPartName());

    // PartName should be different from old part Name.
    assertNotEquals("Part names should be different", partName,
        commitUploadPartInfo.getPartName());
  }

  @Test
  public void testUploadPartOverrideWithRatis() throws IOException {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String sampleData = "sample Value";

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        ReplicationType.RATIS, THREE);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    assertNotNull(multipartInfo.getUploadID());

    int partNumber = 1;

    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), partNumber, uploadID);
    ozoneOutputStream.write(string2Bytes(sampleData), 0, sampleData.length());
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo commitUploadPartInfo = ozoneOutputStream
        .getCommitUploadPartInfo();

    assertNotNull(commitUploadPartInfo);
    String partName = commitUploadPartInfo.getPartName();
    assertNotNull(commitUploadPartInfo.getPartName());

    //Overwrite the part by creating part key with same part number.
    sampleData = "sample Data Changed";
    ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), partNumber, uploadID);
    ozoneOutputStream.write(string2Bytes(sampleData), 0, "name".length());
    ozoneOutputStream.close();

    commitUploadPartInfo = ozoneOutputStream
        .getCommitUploadPartInfo();

    assertNotNull(commitUploadPartInfo);
    assertNotNull(commitUploadPartInfo.getPartName());

    // PartName should be different from old part Name.
    assertNotEquals("Part names should be different", partName,
        commitUploadPartInfo.getPartName());
  }

  @Test
  public void testNoSuchUploadError() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String sampleData = "sample Value";

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String uploadID = "random";
    OzoneTestUtils
        .expectOmException(NO_SUCH_MULTIPART_UPLOAD_ERROR, () ->
            bucket
                .createMultipartKey(keyName, sampleData.length(), 1, uploadID));
  }

  @Test
  public void testMultipartUploadWithACL() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    // Add ACL on Bucket
    OzoneAcl acl1 = new OzoneAcl(USER, "Monday", ACLType.ALL, DEFAULT);
    OzoneAcl acl2 = new OzoneAcl(USER, "Friday", ACLType.ALL, DEFAULT);
    OzoneAcl acl3 = new OzoneAcl(USER, "Jan", ACLType.ALL, ACCESS);
    OzoneAcl acl4 = new OzoneAcl(USER, "Feb", ACLType.ALL, ACCESS);
    bucket.addAcls(acl1);
    bucket.addAcls(acl2);
    bucket.addAcls(acl3);
    bucket.addAcls(acl4);

    doMultipartUpload(bucket, keyName, (byte)98);
    OzoneObj keyObj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName).setKeyName(keyName)
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE).build();
    List<OzoneAcl> aclList = store.getAcl(keyObj);
    // key should inherit bucket's DEFAULT type acl
    Assert.assertTrue(aclList.stream().anyMatch(
        acl -> acl.getName().equals(acl1.getName())));
    Assert.assertTrue(aclList.stream().anyMatch(
        acl -> acl.getName().equals(acl2.getName())));

    // kye should not inherit bucket's ACCESS type acl
    Assert.assertFalse(aclList.stream().anyMatch(
        acl -> acl.getName().equals(acl3.getName())));
    Assert.assertFalse(aclList.stream().anyMatch(
        acl -> acl.getName().equals(acl4.getName())));

    // User without permission should fail to upload the object
    String userName = "test-user";
    UserGroupInformation remoteUser =
        UserGroupInformation.createRemoteUser(userName);
    OzoneClient client =
        remoteUser.doAs((PrivilegedExceptionAction<OzoneClient>)() -> {
          return OzoneClientFactory.getRpcClient(cluster.getConf());
        });
    OzoneAcl acl5 = new OzoneAcl(USER, userName, ACLType.READ, DEFAULT);
    OzoneAcl acl6 = new OzoneAcl(USER, userName, ACLType.READ, ACCESS);
    OzoneObj volumeObj = OzoneObjInfo.Builder.newBuilder()
        .setVolumeName(volumeName).setStoreType(OzoneObj.StoreType.OZONE)
        .setResType(OzoneObj.ResourceType.VOLUME).build();
    OzoneObj bucketObj = OzoneObjInfo.Builder.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setResType(OzoneObj.ResourceType.BUCKET).build();
    store.addAcl(volumeObj, acl5);
    store.addAcl(volumeObj, acl6);
    store.addAcl(bucketObj, acl5);
    store.addAcl(bucketObj, acl6);

    // User without permission cannot start multi-upload
    String keyName2 = UUID.randomUUID().toString();
    OzoneBucket bucket2 = client.getObjectStore().getVolume(volumeName)
        .getBucket(bucketName);
    try {
      initiateMultipartUpload(bucket2, keyName2, ReplicationType.RATIS, THREE);
      fail("User without permission should fail");
    } catch (Exception e) {
      assertTrue(e instanceof OMException);
      assertEquals(ResultCodes.PERMISSION_DENIED,
          ((OMException) e).getResult());
    }

    // Add create permission for user, and try multi-upload init again
    OzoneAcl acl7 = new OzoneAcl(USER, userName, ACLType.CREATE, DEFAULT);
    OzoneAcl acl8 = new OzoneAcl(USER, userName, ACLType.CREATE, ACCESS);
    OzoneAcl acl9 = new OzoneAcl(USER, userName, WRITE, DEFAULT);
    OzoneAcl acl10 = new OzoneAcl(USER, userName, WRITE, ACCESS);
    store.addAcl(volumeObj, acl7);
    store.addAcl(volumeObj, acl8);
    store.addAcl(volumeObj, acl9);
    store.addAcl(volumeObj, acl10);

    store.addAcl(bucketObj, acl7);
    store.addAcl(bucketObj, acl8);
    store.addAcl(bucketObj, acl9);
    store.addAcl(bucketObj, acl10);
    String uploadId = initiateMultipartUpload(bucket2, keyName2,
        ReplicationType.RATIS, THREE);

    // Upload part
    byte[] data = generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)1);
    String partName = uploadPart(bucket, keyName2, uploadId, 1, data);
    Map<Integer, String> partsMap = new TreeMap<>();
    partsMap.put(1, partName);

    // Complete multipart upload request
    completeMultipartUpload(bucket2, keyName2, uploadId, partsMap);

    // User without permission cannot read multi-uploaded object
    try {
      OzoneInputStream inputStream = bucket2.readKey(keyName);
      fail("User without permission should fail");
    } catch (Exception e) {
      assertTrue(e instanceof OMException);
      assertEquals(ResultCodes.PERMISSION_DENIED,
          ((OMException) e).getResult());
    }
  }

  @Test
  public void testMultipartUploadOverride() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    doMultipartUpload(bucket, keyName, (byte)96);

    // Initiate Multipart upload again, now we should read latest version, as
    // read always reads latest blocks.
    doMultipartUpload(bucket, keyName, (byte)97);

  }


  @Test
  public void testMultipartUploadWithPartsLessThanMinSize() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    // Initiate multipart upload
    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);

    // Upload Parts
    Map<Integer, String> partsMap = new TreeMap<>();
    // Uploading part 1 with less than min size
    String partName = uploadPart(bucket, keyName, uploadID, 1,
        "data".getBytes(UTF_8));
    partsMap.put(1, partName);

    partName = uploadPart(bucket, keyName, uploadID, 2,
        "data".getBytes(UTF_8));
    partsMap.put(2, partName);


    // Complete multipart upload

    OzoneTestUtils.expectOmException(ResultCodes.ENTITY_TOO_SMALL,
        () -> completeMultipartUpload(bucket, keyName, uploadID, partsMap));

  }
  @Test
  public void testMultipartUploadWithPartsMisMatchWithListSizeDifferent()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);

    // We have not uploaded any parts, but passing some list it should throw
    // error.
    TreeMap<Integer, String> partsMap = new TreeMap<>();
    partsMap.put(1, UUID.randomUUID().toString());

    OzoneTestUtils.expectOmException(ResultCodes.INVALID_PART,
        () -> completeMultipartUpload(bucket, keyName, uploadID, partsMap));

  }

  @Test
  public void testMultipartUploadWithPartsMisMatchWithIncorrectPartName()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);

    uploadPart(bucket, keyName, uploadID, 1, "data".getBytes(UTF_8));
    // We have not uploaded any parts, but passing some list it should throw
    // error.
    TreeMap<Integer, String> partsMap = new TreeMap<>();
    partsMap.put(1, UUID.randomUUID().toString());

    OzoneTestUtils.expectOmException(ResultCodes.INVALID_PART,
        () -> completeMultipartUpload(bucket, keyName, uploadID, partsMap));

  }

  @Test
  public void testMultipartUploadWithMissingParts() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);

    uploadPart(bucket, keyName, uploadID, 1, "data".getBytes(UTF_8));
    // We have not uploaded any parts, but passing some list it should throw
    // error.
    TreeMap<Integer, String> partsMap = new TreeMap<>();
    partsMap.put(3, "random");

    OzoneTestUtils.expectOmException(ResultCodes.INVALID_PART,
        () -> completeMultipartUpload(bucket, keyName, uploadID, partsMap));
  }

  @Test
  public void testAbortUploadFail() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    OzoneTestUtils.expectOmException(NO_SUCH_MULTIPART_UPLOAD_ERROR,
        () -> bucket.abortMultipartUpload(keyName, "random"));
  }

  @Test
  public void testAbortUploadFailWithInProgressPartUpload() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    OmMultipartInfo omMultipartInfo = bucket.initiateMultipartUpload(keyName,
        STAND_ALONE, ONE);

    Assert.assertNotNull(omMultipartInfo.getUploadID());

    // Do not close output stream.
    byte[] data = "data".getBytes(UTF_8);
    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, 1, omMultipartInfo.getUploadID());
    ozoneOutputStream.write(data, 0, data.length);

    // Abort before completing part upload.
    bucket.abortMultipartUpload(keyName, omMultipartInfo.getUploadID());

    try {
      ozoneOutputStream.close();
      fail("testAbortUploadFailWithInProgressPartUpload failed");
    } catch (IOException ex) {
      assertTrue(ex instanceof OMException);
      assertEquals(NO_SUCH_MULTIPART_UPLOAD_ERROR,
          ((OMException) ex).getResult());
    }
  }

  @Test
  public void testCommitPartAfterCompleteUpload() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    OmMultipartInfo omMultipartInfo = bucket.initiateMultipartUpload(keyName,
        STAND_ALONE, ONE);

    Assert.assertNotNull(omMultipartInfo.getUploadID());

    String uploadID = omMultipartInfo.getUploadID();

    // upload part 1.
    byte[] data = generateData(5 * 1024 * 1024,
        (byte) RandomUtils.nextLong());
    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, 1, uploadID);
    ozoneOutputStream.write(data, 0, data.length);
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
        ozoneOutputStream.getCommitUploadPartInfo();

    // Do not close output stream for part 2.
    ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, 2, omMultipartInfo.getUploadID());
    ozoneOutputStream.write(data, 0, data.length);

    Map<Integer, String> partsMap = new LinkedHashMap<>();
    partsMap.put(1, omMultipartCommitUploadPartInfo.getPartName());
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo =
        bucket.completeMultipartUpload(keyName,
        uploadID, partsMap);

    Assert.assertNotNull(omMultipartCommitUploadPartInfo);

    byte[] fileContent = new byte[data.length];
    OzoneInputStream inputStream = bucket.readKey(keyName);
    inputStream.read(fileContent);
    StringBuilder sb = new StringBuilder(data.length);

    // Combine all parts data, and check is it matching with get key data.
    String part1 = new String(data, UTF_8);
    sb.append(part1);
    Assert.assertEquals(sb.toString(), new String(fileContent, UTF_8));

    try {
      ozoneOutputStream.close();
      fail("testCommitPartAfterCompleteUpload failed");
    } catch (IOException ex) {
      assertTrue(ex instanceof OMException);
      assertEquals(NO_SUCH_MULTIPART_UPLOAD_ERROR,
          ((OMException) ex).getResult());
    }
  }


  @Test
  public void testAbortUploadSuccessWithOutAnyParts() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);
    bucket.abortMultipartUpload(keyName, uploadID);
  }

  @Test
  public void testAbortUploadSuccessWithParts() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);
    uploadPart(bucket, keyName, uploadID, 1, "data".getBytes(UTF_8));
    bucket.abortMultipartUpload(keyName, uploadID);
  }

  @Test
  public void testListMultipartUploadParts() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    Map<Integer, String> partsMap = new TreeMap<>();
    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);
    String partName1 = uploadPart(bucket, keyName, uploadID, 1,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(1, partName1);

    String partName2 =uploadPart(bucket, keyName, uploadID, 2,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(2, partName2);

    String partName3 =uploadPart(bucket, keyName, uploadID, 3,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(3, partName3);

    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 0, 3);

    Assert.assertEquals(STAND_ALONE,
        ozoneMultipartUploadPartListParts.getReplicationType());
    Assert.assertEquals(3,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());

    Assert.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(0).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(0)
            .getPartName());
    Assert.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(1).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(1)
            .getPartName());
    Assert.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(2).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(2)
            .getPartName());

    Assert.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());
  }

  @Test
  public void testListMultipartUploadPartsWithContinuation()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    Map<Integer, String> partsMap = new TreeMap<>();
    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);
    String partName1 = uploadPart(bucket, keyName, uploadID, 1,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(1, partName1);

    String partName2 =uploadPart(bucket, keyName, uploadID, 2,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(2, partName2);

    String partName3 =uploadPart(bucket, keyName, uploadID, 3,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(3, partName3);

    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 0, 2);

    Assert.assertEquals(STAND_ALONE,
        ozoneMultipartUploadPartListParts.getReplicationType());

    Assert.assertEquals(2,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());

    Assert.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(0).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(0)
            .getPartName());
    Assert.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(1).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(1)
            .getPartName());

    // Get remaining
    Assert.assertTrue(ozoneMultipartUploadPartListParts.isTruncated());
    ozoneMultipartUploadPartListParts = bucket.listParts(keyName, uploadID,
        ozoneMultipartUploadPartListParts.getNextPartNumberMarker(), 2);

    Assert.assertEquals(1,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());
    Assert.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(0).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(0)
            .getPartName());


    // As we don't have any parts for this, we should get false here
    Assert.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());

  }

  @Test
  public void testListPartsInvalidPartMarker() throws Exception {
    try {
      String volumeName = UUID.randomUUID().toString();
      String bucketName = UUID.randomUUID().toString();
      String keyName = UUID.randomUUID().toString();

      store.createVolume(volumeName);
      OzoneVolume volume = store.getVolume(volumeName);
      volume.createBucket(bucketName);
      OzoneBucket bucket = volume.getBucket(bucketName);


      OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
          bucket.listParts(keyName, "random", -1, 2);
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains("Should be greater than or " +
          "equal to zero", ex);
    }
  }

  @Test
  public void testListPartsInvalidMaxParts() throws Exception {
    try {
      String volumeName = UUID.randomUUID().toString();
      String bucketName = UUID.randomUUID().toString();
      String keyName = UUID.randomUUID().toString();

      store.createVolume(volumeName);
      OzoneVolume volume = store.getVolume(volumeName);
      volume.createBucket(bucketName);
      OzoneBucket bucket = volume.getBucket(bucketName);


      OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
          bucket.listParts(keyName, "random", 1,  -1);
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains("Max Parts Should be greater " +
          "than zero", ex);
    }
  }

  @Test
  public void testListPartsWithPartMarkerGreaterThanPartCount()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);


    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);
    uploadPart(bucket, keyName, uploadID, 1,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));


    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 100, 2);

    // Should return empty

    Assert.assertEquals(0,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());
    Assert.assertEquals(STAND_ALONE,
        ozoneMultipartUploadPartListParts.getReplicationType());

    // As we don't have any parts with greater than partNumberMarker and list
    // is not truncated, so it should return false here.
    Assert.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());

  }

  @Test
  public void testListPartsWithInvalidUploadID() throws Exception {
    OzoneTestUtils
        .expectOmException(NO_SUCH_MULTIPART_UPLOAD_ERROR, () -> {
          String volumeName = UUID.randomUUID().toString();
          String bucketName = UUID.randomUUID().toString();
          String keyName = UUID.randomUUID().toString();

          store.createVolume(volumeName);
          OzoneVolume volume = store.getVolume(volumeName);
          volume.createBucket(bucketName);
          OzoneBucket bucket = volume.getBucket(bucketName);
          OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
              bucket.listParts(keyName, "random", 100, 2);
        });
  }

  @Test
  public void testNativeAclsForVolume() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);

    OzoneObj ozObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setResType(OzoneObj.ResourceType.VOLUME)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    validateOzoneAccessAcl(ozObj);
  }

  @Test
  public void testNativeAclsForBucket() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertNotNull("Bucket creation failed", bucket);

    OzoneObj ozObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    validateOzoneAccessAcl(ozObj);

    OzoneObj volObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setResType(OzoneObj.ResourceType.VOLUME)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();
    validateDefaultAcls(volObj, ozObj, volume, null);
  }

  private void validateDefaultAcls(OzoneObj parentObj, OzoneObj childObj,
      OzoneVolume volume,  OzoneBucket bucket) throws Exception {
    assertTrue(store.addAcl(parentObj, defaultUserAcl));
    assertTrue(store.addAcl(parentObj, defaultGroupAcl));
    if (volume != null) {
      volume.deleteBucket(childObj.getBucketName());
      volume.createBucket(childObj.getBucketName());
    } else {
      if (childObj.getResourceType().equals(OzoneObj.ResourceType.KEY)) {
        bucket.deleteKey(childObj.getKeyName());
        writeKey(childObj.getKeyName(), bucket);
      } else {
        store.setAcl(childObj, getAclList(new OzoneConfiguration()));
      }
    }
    List<OzoneAcl> acls = store.getAcl(parentObj);
    assertTrue("Current acls: " + StringUtils.join(",", acls) +
            " inheritedUserAcl: " + inheritedUserAcl,
        acls.contains(defaultUserAcl));
    assertTrue("Current acls: " + StringUtils.join(",", acls) +
            " inheritedGroupAcl: " + inheritedGroupAcl,
        acls.contains(defaultGroupAcl));

    acls = store.getAcl(childObj);
    assertTrue("Current acls:" + StringUtils.join(",", acls) +
            " inheritedUserAcl:" + inheritedUserAcl,
        acls.contains(inheritedUserAcl));
    assertTrue("Current acls:" + StringUtils.join(",", acls) +
            " inheritedGroupAcl:" + inheritedGroupAcl,
        acls.contains(inheritedGroupAcl));
  }

  @Test
  public void testNativeAclsForKey() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String key1 = "dir1/dir2" + UUID.randomUUID().toString();
    String key2 = "dir1/dir2" + UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertNotNull("Bucket creation failed", bucket);

    writeKey(key1, bucket);
    writeKey(key2, bucket);

    OzoneObj ozObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(key1)
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    // Validates access acls.
    validateOzoneAccessAcl(ozObj);

    // Check default acls inherited from bucket.
    OzoneObj buckObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(key1)
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    validateDefaultAcls(buckObj, ozObj, null, bucket);

    // Check default acls inherited from prefix.
    OzoneObj prefixObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(key1)
        .setPrefixName("dir1/")
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();
    store.setAcl(prefixObj, getAclList(new OzoneConfiguration()));
    // Prefix should inherit DEFAULT acl from bucket.

    List<OzoneAcl> acls = store.getAcl(prefixObj);
    assertTrue("Current acls:" + StringUtils.join(",", acls),
        acls.contains(inheritedUserAcl));
    assertTrue("Current acls:" + StringUtils.join(",", acls),
        acls.contains(inheritedGroupAcl));
    // Remove inherited acls from prefix.
    assertTrue(store.removeAcl(prefixObj, inheritedUserAcl));
    assertTrue(store.removeAcl(prefixObj, inheritedGroupAcl));

    validateDefaultAcls(prefixObj, ozObj, null, bucket);
  }

  @Test
  public void testNativeAclsForPrefix() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String prefix1 = "PF" + UUID.randomUUID().toString() + "/";
    String key1 = prefix1 + "KEY" + UUID.randomUUID().toString();

    String prefix2 = "PF" + UUID.randomUUID().toString() + "/";
    String key2 = prefix2 + "KEY" + UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertNotNull("Bucket creation failed", bucket);

    writeKey(key1, bucket);
    writeKey(key2, bucket);

    OzoneObj prefixObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(prefix1)
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    OzoneObj prefixObj2 = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(prefix2)
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    // add acl
    BitSet aclRights1 = new BitSet();
    aclRights1.set(READ.ordinal());
    OzoneAcl user1Acl = new OzoneAcl(USER,
        "user1", aclRights1, ACCESS);
    assertTrue(store.addAcl(prefixObj, user1Acl));

    // get acl
    List<OzoneAcl> aclsGet = store.getAcl(prefixObj);
    Assert.assertEquals(1, aclsGet.size());
    Assert.assertEquals(user1Acl, aclsGet.get(0));

    // remove acl
    Assert.assertTrue(store.removeAcl(prefixObj, user1Acl));
    aclsGet = store.getAcl(prefixObj);
    Assert.assertEquals(0, aclsGet.size());

    // set acl
    BitSet aclRights2 = new BitSet();
    aclRights2.set(ACLType.ALL.ordinal());
    OzoneAcl group1Acl = new OzoneAcl(GROUP,
        "group1", aclRights2, ACCESS);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(user1Acl);
    acls.add(group1Acl);
    Assert.assertTrue(store.setAcl(prefixObj, acls));

    // get acl
    aclsGet = store.getAcl(prefixObj);
    Assert.assertEquals(2, aclsGet.size());

    OzoneObj keyObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(key1)
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    // Check default acls inherited from prefix.
    validateDefaultAcls(prefixObj, keyObj, null, bucket);

    // Check default acls inherited from bucket when prefix does not exist.
    validateDefaultAcls(prefixObj2, keyObj, null, bucket);
  }

  /**
   * Helper function to get default acl list for current user.
   *
   * @return list of default Acls.
   * @throws IOException
   * */
  private List<OzoneAcl> getAclList(OzoneConfiguration conf)
      throws IOException {
    List<OzoneAcl> listOfAcls = new ArrayList<>();
    //User ACL
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    OzoneAclConfig aclConfig = conf.getObject(OzoneAclConfig.class);
    ACLType userRights = aclConfig.getUserDefaultRights();
    ACLType groupRights = aclConfig.getGroupDefaultRights();

    listOfAcls.add(new OzoneAcl(USER,
        ugi.getUserName(), userRights, ACCESS));
    //Group ACLs of the User
    List<String> userGroups = Arrays.asList(ugi.getGroupNames());
    userGroups.stream().forEach((group) -> listOfAcls.add(
        new OzoneAcl(GROUP, group, groupRights, ACCESS)));
    return listOfAcls;
  }

  /**
   * Helper function to validate ozone Acl for given object.
   * @param ozObj
   * */
  private void validateOzoneAccessAcl(OzoneObj ozObj) throws IOException {
    // Get acls for volume.
    List<OzoneAcl> expectedAcls = getAclList(new OzoneConfiguration());

    // Case:1 Add new acl permission to existing acl.
    if(expectedAcls.size()>0) {
      OzoneAcl oldAcl = expectedAcls.get(0);
      OzoneAcl newAcl = new OzoneAcl(oldAcl.getType(), oldAcl.getName(),
          ACLType.READ_ACL, oldAcl.getAclScope());
      // Verify that operation successful.
      assertTrue(store.addAcl(ozObj, newAcl));

      assertEquals(expectedAcls.size(), store.getAcl(ozObj).size());
      final Optional<OzoneAcl> readAcl = store.getAcl(ozObj).stream()
          .filter(acl -> acl.getName().equals(newAcl.getName())
              && acl.getType().equals(newAcl.getType()))
          .findFirst();
      assertTrue("New acl expected but not found.", readAcl.isPresent());
      assertTrue("READ_ACL should exist in current acls:"
          + readAcl.get(),
          readAcl.get().getAclList().contains(ACLType.READ_ACL));


      // Case:2 Remove newly added acl permission.
      assertTrue(store.removeAcl(ozObj, newAcl));

      assertEquals(expectedAcls.size(), store.getAcl(ozObj).size());
      final Optional<OzoneAcl> nonReadAcl = store.getAcl(ozObj).stream()
          .filter(acl -> acl.getName().equals(newAcl.getName())
              && acl.getType().equals(newAcl.getType()))
          .findFirst();
      assertTrue("New acl expected but not found.", nonReadAcl.isPresent());
      assertFalse("READ_ACL should not exist in current acls:"
              + nonReadAcl.get(),
          nonReadAcl.get().getAclList().contains(ACLType.READ_ACL));
    } else {
      fail("Default acl should not be empty.");
    }

    List<OzoneAcl> keyAcls = store.getAcl(ozObj);
    expectedAcls.forEach(a -> assertTrue(keyAcls.contains(a)));

    // Remove all acl's.
    for (OzoneAcl a : expectedAcls) {
      store.removeAcl(ozObj, a);
    }
    List<OzoneAcl> newAcls = store.getAcl(ozObj);
    assertEquals(0, newAcls.size());

    // Add acl's and then call getAcl.
    int aclCount = 0;
    for (OzoneAcl a : expectedAcls) {
      aclCount++;
      assertTrue(store.addAcl(ozObj, a));
      assertEquals(aclCount, store.getAcl(ozObj).size());
    }
    newAcls = store.getAcl(ozObj);
    assertEquals(expectedAcls.size(), newAcls.size());
    List<OzoneAcl> finalNewAcls = newAcls;
    expectedAcls.forEach(a -> assertTrue(finalNewAcls.contains(a)));

    // Reset acl's.
    OzoneAcl ua = new OzoneAcl(USER, "userx",
        ACLType.READ_ACL, ACCESS);
    OzoneAcl ug = new OzoneAcl(GROUP, "userx",
        ACLType.ALL, ACCESS);
    store.setAcl(ozObj, Arrays.asList(ua, ug));
    newAcls = store.getAcl(ozObj);
    assertEquals(2, newAcls.size());
    assertTrue(newAcls.contains(ua));
    assertTrue(newAcls.contains(ug));
  }

  private void writeKey(String key1, OzoneBucket bucket) throws IOException {
    OzoneOutputStream out = bucket.createKey(key1, 1024, STAND_ALONE,
        ONE, new HashMap<>());
    out.write(RandomStringUtils.random(1024).getBytes(UTF_8));
    out.close();
  }

  private byte[] generateData(int size, byte val) {
    byte[] chars = new byte[size];
    Arrays.fill(chars, val);
    return chars;
  }

  private void doMultipartUpload(OzoneBucket bucket, String keyName, byte val)
      throws Exception {
    // Initiate Multipart upload request
    String uploadID = initiateMultipartUpload(bucket, keyName, ReplicationType
        .RATIS, THREE);

    // Upload parts
    Map<Integer, String> partsMap = new TreeMap<>();

    // get 5mb data, as each part should be of min 5mb, last part can be less
    // than 5mb
    int length = 0;
    byte[] data = generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, val);
    String partName = uploadPart(bucket, keyName, uploadID, 1, data);
    partsMap.put(1, partName);
    length += data.length;


    partName = uploadPart(bucket, keyName, uploadID, 2, data);
    partsMap.put(2, partName);
    length += data.length;

    String part3 = UUID.randomUUID().toString();
    partName = uploadPart(bucket, keyName, uploadID, 3, part3.getBytes(
        UTF_8));
    partsMap.put(3, partName);
    length += part3.getBytes(UTF_8).length;

    // Complete multipart upload request
    completeMultipartUpload(bucket, keyName, uploadID, partsMap);

    //Now Read the key which has been completed multipart upload.
    byte[] fileContent = new byte[data.length + data.length + part3.getBytes(
        UTF_8).length];
    OzoneInputStream inputStream = bucket.readKey(keyName);
    inputStream.read(fileContent);

    Assert.assertTrue(verifyRatisReplication(bucket.getVolumeName(),
        bucket.getName(), keyName, ReplicationType.RATIS,
        THREE));

    StringBuilder sb = new StringBuilder(length);

    // Combine all parts data, and check is it matching with get key data.
    String part1 = new String(data, UTF_8);
    String part2 = new String(data, UTF_8);
    sb.append(part1);
    sb.append(part2);
    sb.append(part3);
    Assert.assertEquals(sb.toString(), new String(fileContent, UTF_8));

    String ozoneKey = ozoneManager.getMetadataManager()
        .getOzoneKey(bucket.getVolumeName(), bucket.getName(), keyName);
    OmKeyInfo omKeyInfo = ozoneManager.getMetadataManager().getKeyTable()
        .get(ozoneKey);

    OmKeyLocationInfoGroup latestVersionLocations =
        omKeyInfo.getLatestVersionLocations();
    Assert.assertEquals(true, latestVersionLocations.isMultipartKey());
    latestVersionLocations.getBlocksLatestVersionOnly()
        .forEach(omKeyLocationInfo ->
            Assert.assertTrue(omKeyLocationInfo.getPartNumber() != -1));
  }

  private String initiateMultipartUpload(OzoneBucket bucket, String keyName,
      ReplicationType replicationType, ReplicationFactor replicationFactor)
      throws Exception {
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        replicationType, replicationFactor);

    String uploadID = multipartInfo.getUploadID();
    Assert.assertNotNull(uploadID);
    return uploadID;
  }

  private String uploadPart(OzoneBucket bucket, String keyName, String
      uploadID, int partNumber, byte[] data) throws Exception {
    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, partNumber, uploadID);
    ozoneOutputStream.write(data, 0,
        data.length);
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
        ozoneOutputStream.getCommitUploadPartInfo();

    Assert.assertNotNull(omMultipartCommitUploadPartInfo);
    Assert.assertNotNull(omMultipartCommitUploadPartInfo.getPartName());
    return omMultipartCommitUploadPartInfo.getPartName();

  }

  private void completeMultipartUpload(OzoneBucket bucket, String keyName,
      String uploadID, Map<Integer, String> partsMap) throws Exception {
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo = bucket
        .completeMultipartUpload(keyName, uploadID, partsMap);

    Assert.assertNotNull(omMultipartUploadCompleteInfo);
    Assert.assertEquals(omMultipartUploadCompleteInfo.getBucket(), bucket
        .getName());
    Assert.assertEquals(omMultipartUploadCompleteInfo.getVolume(), bucket
        .getVolumeName());
    Assert.assertEquals(omMultipartUploadCompleteInfo.getKey(), keyName);
    Assert.assertNotNull(omMultipartUploadCompleteInfo.getHash());
  }

  private void createTestKey(OzoneBucket bucket, String keyName,
                             String keyValue) throws IOException {
    OzoneOutputStream out = bucket.createKey(keyName,
        keyValue.getBytes(UTF_8).length, STAND_ALONE,
        ONE, new HashMap<>());
    out.write(keyValue.getBytes(UTF_8));
    out.close();
    OzoneKey key = bucket.getKey(keyName);
    Assert.assertEquals(keyName, key.getName());
  }

  private void assertKeyRenamedEx(OzoneBucket bucket, String keyName)
      throws Exception {
    OMException oe = null;
    try {
      bucket.getKey(keyName);
    } catch (OMException e) {
      oe = e;
    }
    Assert.assertEquals(KEY_NOT_FOUND, oe.getResult());
  }

  /**
   * Tests GDPR encryption/decryption.
   * 1. Create GDPR Enabled bucket.
   * 2. Create a Key in this bucket so it gets encrypted via GDPRSymmetricKey.
   * 3. Read key and validate the content/metadata is as expected because the
   * readKey will decrypt using the GDPR Symmetric Key with details from KeyInfo
   * Metadata.
   * 4. To check encryption, we forcibly update KeyInfo Metadata and remove the
   * gdprEnabled flag
   * 5. When we now read the key, {@link RpcClient} checks for GDPR Flag in
   * method createInputStream. If the gdprEnabled flag in metadata is set to
   * true, it decrypts using the GDPRSymmetricKey. Since we removed that flag
   * from metadata for this key, if will read the encrypted data as-is.
   * 6. Thus, when we compare this content with expected text, it should
   * not match as the decryption has not been performed.
   * @throws Exception
   */
  @Test
  public void testKeyReadWriteForGDPR() throws Exception {
    //Step 1
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs args = BucketArgs.newBuilder()
        .addMetadata(OzoneConsts.GDPR_FLAG, "true").build();
    volume.createBucket(bucketName, args);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertNotNull(bucket.getMetadata());
    Assert.assertEquals("true",
        bucket.getMetadata().get(OzoneConsts.GDPR_FLAG));

    //Step 2
    String text = "hello world";
    Map<String, String> keyMetadata = new HashMap<>();
    keyMetadata.put(OzoneConsts.GDPR_FLAG, "true");
    OzoneOutputStream out = bucket.createKey(keyName,
        text.getBytes(UTF_8).length, STAND_ALONE, ONE, keyMetadata);
    out.write(text.getBytes(UTF_8));
    out.close();
    Assert.assertNull(keyMetadata.get(OzoneConsts.GDPR_SECRET));

    //Step 3
    OzoneKeyDetails key = bucket.getKey(keyName);

    Assert.assertEquals(keyName, key.getName());
    Assert.assertEquals("true", key.getMetadata().get(OzoneConsts.GDPR_FLAG));
    Assert.assertEquals("AES",
        key.getMetadata().get(OzoneConsts.GDPR_ALGORITHM));
    Assert.assertNotNull(key.getMetadata().get(OzoneConsts.GDPR_SECRET));

    OzoneInputStream is = bucket.readKey(keyName);
    byte[] fileContent = new byte[text.getBytes(UTF_8).length];
    is.read(fileContent);
    Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
        keyName, STAND_ALONE,
        ONE));
    Assert.assertEquals(text, new String(fileContent, UTF_8));

    //Step 4
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable().get(omMetadataManager.getOzoneKey(
            volumeName, bucketName, keyName));

    omKeyInfo.getMetadata().remove(OzoneConsts.GDPR_FLAG);

    omMetadataManager.getKeyTable().put(omMetadataManager.getOzoneKey(
         volumeName, bucketName, keyName), omKeyInfo);

    //Step 5
    key = bucket.getKey(keyName);
    Assert.assertEquals(keyName, key.getName());
    Assert.assertNull(key.getMetadata().get(OzoneConsts.GDPR_FLAG));
    is = bucket.readKey(keyName);
    fileContent = new byte[text.getBytes(UTF_8).length];
    is.read(fileContent);

    //Step 6
    Assert.assertNotEquals(text, new String(fileContent, UTF_8));

  }

  /**
   * Tests deletedKey for GDPR.
   * 1. Create GDPR Enabled bucket.
   * 2. Create a Key in this bucket so it gets encrypted via GDPRSymmetricKey.
   * 3. Read key and validate the content/metadata is as expected because the
   * readKey will decrypt using the GDPR Symmetric Key with details from KeyInfo
   * Metadata.
   * 4. Delete this key in GDPR enabled bucket
   * 5. Confirm the deleted key metadata in deletedTable does not contain the
   * GDPR encryption details (flag, secret, algorithm).
   * @throws Exception
   */
  @Test
  public void testDeletedKeyForGDPR() throws Exception {
    //Step 1
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs args = BucketArgs.newBuilder()
        .addMetadata(OzoneConsts.GDPR_FLAG, "true").build();
    volume.createBucket(bucketName, args);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertNotNull(bucket.getMetadata());
    Assert.assertEquals("true",
        bucket.getMetadata().get(OzoneConsts.GDPR_FLAG));

    //Step 2
    String text = "hello world";
    Map<String, String> keyMetadata = new HashMap<>();
    keyMetadata.put(OzoneConsts.GDPR_FLAG, "true");
    OzoneOutputStream out = bucket.createKey(keyName,
        text.getBytes(UTF_8).length, STAND_ALONE, ONE, keyMetadata);
    out.write(text.getBytes(UTF_8));
    out.close();

    //Step 3
    OzoneKeyDetails key = bucket.getKey(keyName);

    Assert.assertEquals(keyName, key.getName());
    Assert.assertEquals("true", key.getMetadata().get(OzoneConsts.GDPR_FLAG));
    Assert.assertEquals("AES",
        key.getMetadata().get(OzoneConsts.GDPR_ALGORITHM));
    Assert.assertTrue(key.getMetadata().get(OzoneConsts.GDPR_SECRET) != null);

    OzoneInputStream is = bucket.readKey(keyName);
    byte[] fileContent = new byte[text.getBytes(UTF_8).length];
    is.read(fileContent);
    Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
        keyName, STAND_ALONE,
        ONE));
    Assert.assertEquals(text, new String(fileContent, UTF_8));

    //Step 4
    bucket.deleteKey(keyName);

    //Step 5
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    String objectKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);
    RepeatedOmKeyInfo deletedKeys =
        omMetadataManager.getDeletedTable().get(objectKey);
    if (deletedKeys != null) {
      Map<String, String> deletedKeyMetadata =
          deletedKeys.getOmKeyInfoList().get(0).getMetadata();
      Assert.assertFalse(deletedKeyMetadata.containsKey(OzoneConsts.GDPR_FLAG));
      Assert.assertFalse(
          deletedKeyMetadata.containsKey(OzoneConsts.GDPR_SECRET));
      Assert.assertFalse(
          deletedKeyMetadata.containsKey(OzoneConsts.GDPR_ALGORITHM));
    }
  }


  @Test
  public void setS3VolumeAcl() throws Exception {
    OzoneObj s3vVolume = new OzoneObjInfo.Builder()
        .setVolumeName(HddsClientUtils.getS3VolumeName(cluster.getConf()))
        .setResType(OzoneObj.ResourceType.VOLUME)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    OzoneAcl ozoneAcl = new OzoneAcl(USER, remoteUserName, WRITE, DEFAULT);

    boolean result = store.addAcl(s3vVolume, ozoneAcl);

    Assert.assertTrue("SetAcl on default s3v failed", result);

    List<OzoneAcl> ozoneAclList = store.getAcl(s3vVolume);

    Assert.assertTrue(ozoneAclList.contains(ozoneAcl));
  }
}
