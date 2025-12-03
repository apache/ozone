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

package org.apache.hadoop.ozone.client.rpc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hdds.StringUtils.string2Bytes;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.utils.ClusterContainersUtil.corruptData;
import static org.apache.hadoop.hdds.utils.ClusterContainersUtil.getContainerByID;
import static org.apache.hadoop.ozone.OmUtils.MAX_TRXN_ID;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.DEFAULT_OM_UPDATE_ID;
import static org.apache.hadoop.ozone.OzoneConsts.ETAG;
import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.apache.hadoop.ozone.OzoneConsts.MD5_HASH;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.client.OzoneClientTestUtils.assertKeyContent;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_GENERATION_MISMATCH;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PARTIAL_RENAME;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.GROUP;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.ALL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.apache.ozone.test.GenericTestUtils.getTestStartTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.slf4j.event.Level.DEBUG;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig.EcCodec;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.audit.AuditLogTestUtils;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneSnapshot;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OmTestUtil;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.S3SecretManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.ha.OMProxyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerStateMachine;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ozone.test.OzoneTestBase;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ozone.test.tag.Unhealthy;
import org.apache.ratis.util.function.CheckedSupplier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an abstract class to test all the public facing APIs of Ozone
 * Client.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
abstract class OzoneRpcClientTests extends OzoneTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(OzoneRpcClientTests.class);

  private static MiniOzoneCluster cluster = null;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  private static OzoneManager ozoneManager;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static String remoteUserName = "remoteUser";
  private static final String REMOTE_GROUP_NAME = "remoteGroup";
  private static OzoneAcl defaultUserAcl = OzoneAcl.of(USER, remoteUserName,
      DEFAULT, READ);
  private static OzoneAcl defaultGroupAcl = OzoneAcl.of(GROUP, REMOTE_GROUP_NAME,
      DEFAULT, READ);
  private static OzoneAcl inheritedUserAcl = OzoneAcl.of(USER, remoteUserName,
      ACCESS, READ);
  private static OzoneAcl inheritedGroupAcl = OzoneAcl.of(GROUP,
      REMOTE_GROUP_NAME, ACCESS, READ);
  private static MessageDigest eTagProvider;
  private static Set<OzoneClient> ozoneClients = new HashSet<>();
  private static GenericTestUtils.PrintStreamCapturer output;
  private static final BucketLayout VERSIONING_TEST_BUCKET_LAYOUT =
      BucketLayout.OBJECT_STORE;

  @BeforeAll
  public static void initialize() throws NoSuchAlgorithmException, UnsupportedEncodingException {
    eTagProvider = MessageDigest.getInstance(MD5_HASH);
    AuditLogTestUtils.enableAuditLog();
    output = GenericTestUtils.captureOut();
  }

  static void startCluster(OzoneConfiguration conf) throws Exception {
    startCluster(conf, MiniOzoneCluster.newBuilder(conf));
  }

  static void startCluster(OzoneConfiguration conf, MiniOzoneCluster.Builder builder) throws Exception {
    // Reduce long wait time in MiniOzoneClusterImpl#waitForHddsDatanodesStop
    //  for testZReadKeyWithUnhealthyContainerReplica.
    conf.set("ozone.scm.stale.node.interval", "10s");
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_DIR_DELETING_SERVICE_INTERVAL, 1, TimeUnit.SECONDS);

    ClientConfigForTesting.newBuilder(StorageUnit.MB)
        .setDataStreamMinPacketSize(1)
        .applyTo(conf);

    cluster = builder
        .setNumDatanodes(14)
        .build();
    cluster.waitForClusterToBeReady();
    ozClient = OzoneClientFactory.getRpcClient(conf);
    ozoneClients.add(ozClient);
    store = ozClient.getObjectStore();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
    ozoneManager = cluster.getOzoneManager();
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  static void shutdownCluster() {
    org.apache.hadoop.hdds.utils.IOUtils.closeQuietly(ozoneClients);
    ozoneClients.clear();
    org.apache.hadoop.hdds.utils.IOUtils.closeQuietly(output);

    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static void setOzClient(OzoneClient ozClient) {
    ozoneClients.add(ozClient);
    OzoneRpcClientTests.ozClient = ozClient;
  }

  private static void setStore(ObjectStore store) {
    OzoneRpcClientTests.store = store;
  }

  public static ObjectStore getStore() {
    return store;
  }

  public static OzoneClient getClient() {
    return ozClient;
  }

  public static MiniOzoneCluster getCluster() {
    return cluster;
  }

  /**
   * Test OM Proxy Provider.
   */
  @Test
  public void testOMClientProxyProvider() {
    final List<OMProxyInfo<OzoneManagerProtocolPB>> omProxies
        = OmTestUtil.getFailoverProxyProvider(store).getOMProxies();

    // For a non-HA OM service, there should be only one OM proxy.
    assertEquals(1, omProxies.size());
    // The address in OMProxyInfo object, which client will connect to,
    // should match the OM's RPC address.
    assertEquals(omProxies.get(0).getAddress(),
        ozoneManager.getOmRpcServerAddr());
  }

  @Test
  public void testDefaultS3GVolumeExists() throws Exception {
    String s3VolumeName =
        HddsClientUtils.getDefaultS3VolumeName(cluster.getConf());
    OzoneVolume ozoneVolume = store.getVolume(s3VolumeName);
    assertEquals(ozoneVolume.getName(), s3VolumeName);
    OMMetadataManager omMetadataManager =
        cluster.getOzoneManager().getMetadataManager();
    long transactionID = MAX_TRXN_ID + 1;
    long objectID = OmUtils.addEpochToTxId(omMetadataManager.getOmEpoch(),
        transactionID);
    OmVolumeArgs omVolumeArgs =
        cluster.getOzoneManager().getMetadataManager().getVolumeTable().get(
            omMetadataManager.getVolumeKey(s3VolumeName));
    assertEquals(objectID, omVolumeArgs.getObjectID());
    assertEquals(DEFAULT_OM_UPDATE_ID, omVolumeArgs.getUpdateID());
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
  public void testBucketSetOwner() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    store.getVolume(volumeName).createBucket(bucketName);

    String oldOwner = store.getVolume(volumeName).getBucket(bucketName)
        .getOwner();
    String ownerName = "testUser";

    ClientProtocol proxy = store.getClientProxy();
    proxy.setBucketOwner(volumeName, bucketName, ownerName);
    String newOwner = store.getVolume(volumeName).getBucket(bucketName)
        .getOwner();

    assertEquals(ownerName, newOwner);
    assertNotEquals(oldOwner, newOwner);
    store.getVolume(volumeName).deleteBucket(bucketName);
    store.deleteVolume(volumeName);
  }

  @Test void testKeyOwner() throws IOException {
    // Save the old user, and switch to the old user after test
    UserGroupInformation oldUser = UserGroupInformation.getCurrentUser();
    try {
      // user1 create a key key1
      // user1 create a key key2
      UserGroupInformation user1 = UserGroupInformation
          .createUserForTesting("user1", new String[] {"user1"});
      UserGroupInformation user2 = UserGroupInformation
          .createUserForTesting("user2", new String[] {"user2"});
      String key1 = "key1";
      String key2 = "key2";
      String content = "1234567890";
      String volumeName = UUID.randomUUID().toString();
      String bucketName = UUID.randomUUID().toString();
      store.createVolume(volumeName);
      store.getVolume(volumeName).createBucket(bucketName);
      OzoneObj volumeObj = OzoneObjInfo.Builder.newBuilder()
          .setVolumeName(volumeName).setStoreType(OzoneObj.StoreType.OZONE)
          .setResType(OzoneObj.ResourceType.VOLUME).build();
      OzoneObj bucketObj = OzoneObjInfo.Builder.newBuilder()
          .setVolumeName(volumeName).setBucketName(bucketName)
          .setStoreType(OzoneObj.StoreType.OZONE)
          .setResType(OzoneObj.ResourceType.BUCKET).build();
      store.addAcl(volumeObj, OzoneAcl.of(USER, "user1", ACCESS, ALL));
      store.addAcl(volumeObj, OzoneAcl.of(USER, "user2", ACCESS, ALL));
      store.addAcl(bucketObj, OzoneAcl.of(USER, "user1", ACCESS, ALL));
      store.addAcl(bucketObj, OzoneAcl.of(USER, "user2", ACCESS, ALL));

      createKeyForUser(volumeName, bucketName, key1, content, user1);
      createKeyForUser(volumeName, bucketName, key2, content, user2);
      UserGroupInformation.setLoginUser(oldUser);
      setOzClient(OzoneClientFactory.getRpcClient(cluster.getConf()));
      setStore(ozClient.getObjectStore());
      OzoneBucket bucket = store.getVolume(volumeName).getBucket(bucketName);
      assertNotNull(bucket.getKey(key1));
      assertNotNull(bucket.getKey(key2));
      assertEquals(user1.getShortUserName(),
          bucket.getKey(key1).getOwner());
      assertEquals(user2.getShortUserName(),
          bucket.getKey(key2).getOwner());
    } finally {
      UserGroupInformation.setLoginUser(oldUser);
      setOzClient(OzoneClientFactory.getRpcClient(cluster.getConf()));
      setStore(ozClient.getObjectStore());
    }

  }

  private void createKeyForUser(String volumeName, String bucketName,
      String keyName, String keyContent, UserGroupInformation user)
      throws IOException {
    UserGroupInformation.setLoginUser(user);
    setOzClient(OzoneClientFactory.getRpcClient(cluster.getConf()));
    setStore(ozClient.getObjectStore());
    OzoneBucket bucket = store.getVolume(volumeName).getBucket(bucketName);
    createTestKey(bucket, keyName, keyContent);
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

    store.getVolume(volumeName).createBucket(bucketName);
    OzoneBucket bucket = store.getVolume(volumeName).getBucket(bucketName);
    assertEquals(OzoneConsts.QUOTA_RESET, bucket.getQuotaInBytes());
    assertEquals(OzoneConsts.QUOTA_RESET, bucket.getQuotaInNamespace());

    store.getVolume(volumeName).getBucket(bucketName).setQuota(
        OzoneQuota.parseQuota("1GB", "1000"));
    OzoneBucket ozoneBucket = store.getVolume(volumeName).getBucket(bucketName);
    assertEquals(1024 * 1024 * 1024,
        ozoneBucket.getQuotaInBytes());
    assertEquals(1000L, ozoneBucket.getQuotaInNamespace());

    store.getVolume(volumeName).createBucket(bucketName2);
    store.getVolume(volumeName).getBucket(bucketName2).setQuota(
        OzoneQuota.parseQuota("1024", "1000"));
    OzoneBucket ozoneBucket2 =
        store.getVolume(volumeName).getBucket(bucketName2);
    assertEquals(1024L, ozoneBucket2.getQuotaInBytes());

    store.getVolume(volumeName).setQuota(OzoneQuota.parseQuota(
        "10GB", "10000"));
    volume = store.getVolume(volumeName);
    assertEquals(10 * GB, volume.getQuotaInBytes());
    assertEquals(10000L, volume.getQuotaInNamespace());

    IOException ioException = assertThrows(IOException.class,
        ozoneBucket::clearSpaceQuota);
    assertEquals("Can not clear bucket spaceQuota because volume" +
        " spaceQuota is not cleared.", ioException.getMessage());

    writeKey(bucket, UUID.randomUUID().toString(), ONE, value, valueLength);
    assertEquals(1L,
        store.getVolume(volumeName).getBucket(bucketName).getUsedNamespace());
    assertEquals(valueLength,
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());
    assertEquals(2L,
        store.getVolume(volumeName).getUsedNamespace());

    store.getVolume(volumeName).clearSpaceQuota();
    store.getVolume(volumeName).clearNamespaceQuota();
    OzoneVolume clrVolume = store.getVolume(volumeName);
    assertEquals(OzoneConsts.QUOTA_RESET, clrVolume.getQuotaInBytes());
    assertEquals(OzoneConsts.QUOTA_RESET,
        clrVolume.getQuotaInNamespace());

    ozoneBucket.clearSpaceQuota();
    ozoneBucket.clearNamespaceQuota();
    OzoneBucket clrBucket = store.getVolume(volumeName).getBucket(bucketName);
    assertEquals(OzoneConsts.QUOTA_RESET, clrBucket.getQuotaInBytes());
    assertEquals(OzoneConsts.QUOTA_RESET,
        clrBucket.getQuotaInNamespace());
    assertEquals(1L,
        store.getVolume(volumeName).getBucket(bucketName).getUsedNamespace());
    assertEquals(valueLength,
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());
    assertEquals(2L,
        store.getVolume(volumeName).getUsedNamespace());
  }

  @Test
  public void testSetBucketQuotaIllegal() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    store.getVolume(volumeName).createBucket(bucketName);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class,
            () -> store.getVolume(volumeName).getBucket(bucketName)
                .setQuota(OzoneQuota.parseQuota("0GB", "10")));
    assertTrue(exception.getMessage()
        .startsWith("Invalid value for space quota"));

    exception = assertThrows(IllegalArgumentException.class,
        () -> store.getVolume(volumeName).getBucket(bucketName).setQuota(
            OzoneQuota.parseQuota("10GB", "0")));
    assertTrue(exception.getMessage()
        .startsWith("Invalid value for namespace quota"));

    exception = assertThrows(IllegalArgumentException.class,
        () -> store.getVolume(volumeName).getBucket(bucketName).setQuota(
            OzoneQuota.parseQuota("1GB", "-100")));
    assertTrue(exception.getMessage()
        .startsWith("Invalid value for namespace quota"));

    exception = assertThrows(IllegalArgumentException.class,
        () -> store.getVolume(volumeName).getBucket(bucketName).setQuota(
            OzoneQuota.parseQuota("1TEST", "100")));
    assertTrue(exception.getMessage()
        .startsWith("1TEST is invalid."));

    exception = assertThrows(IllegalArgumentException.class,
        () -> store.getVolume(volumeName).getBucket(bucketName).setQuota(
            OzoneQuota.parseQuota("9223372036854775808 BYTES", "100")));
    assertTrue(exception.getMessage()
        .startsWith("9223372036854775808 BYTES is invalid."));

    exception = assertThrows(IllegalArgumentException.class,
        () -> store.getVolume(volumeName).getBucket(bucketName).setQuota(
            OzoneQuota.parseQuota("-10GB", "100")));
    assertTrue(exception.getMessage()
        .startsWith("Invalid value for space quota"));

  }

  @Test
  public void testSetVolumeQuota() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    assertEquals(OzoneConsts.QUOTA_RESET,
        store.getVolume(volumeName).getQuotaInBytes());
    assertEquals(OzoneConsts.QUOTA_RESET,
        store.getVolume(volumeName).getQuotaInNamespace());
    store.getVolume(volumeName).setQuota(OzoneQuota.parseQuota("1GB", "1000"));
    OzoneVolume volume = store.getVolume(volumeName);
    assertEquals(1024 * 1024 * 1024,
        volume.getQuotaInBytes());
    assertEquals(1000L, volume.getQuotaInNamespace());
  }

  @Test
  public void testSetVolumeQuotaIllegal() throws Exception {
    String volumeName = UUID.randomUUID().toString();

    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .addMetadata("key1", "val1")
        .setQuotaInNamespace(0)
        .setQuotaInBytes(0)
        .build();
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> store.createVolume(volumeName, volumeArgs));
    assertTrue(exception.getMessage()
        .startsWith("Invalid values for quota"));

    store.createVolume(volumeName);

    // test volume set quota 0
    exception = assertThrows(IllegalArgumentException.class,
        () -> store.getVolume(volumeName).setQuota(OzoneQuota.parseQuota(
            "0GB", "10")));
    assertTrue(exception.getMessage()
        .startsWith("Invalid value for space quota"));
    exception = assertThrows(IllegalArgumentException.class,
        () -> store.getVolume(volumeName).setQuota(OzoneQuota.parseQuota(
            "10GB", "0")));
    assertTrue(exception.getMessage()
        .startsWith("Invalid value for namespace quota"));

    // The unit should be legal.
    exception = assertThrows(IllegalArgumentException.class,
        () -> store.getVolume(volumeName).setQuota(OzoneQuota.parseQuota(
            "1TEST", "1000")));
    assertTrue(exception.getMessage()
        .startsWith("1TEST is invalid."));

    // The setting value cannot be greater than LONG.MAX_VALUE BYTES.
    exception = assertThrows(IllegalArgumentException.class,
        () -> store.getVolume(volumeName).setQuota(OzoneQuota.parseQuota(
            "9223372036854775808 B", "1000")));
    assertTrue(exception.getMessage()
        .startsWith("9223372036854775808 B is invalid."));

    // The value cannot be negative.
    exception = assertThrows(IllegalArgumentException.class,
        () -> store.getVolume(volumeName).setQuota(OzoneQuota.parseQuota(
            "-10GB", "1000")));
    assertTrue(exception.getMessage()
        .startsWith("Invalid value for space quota"));
  }

  @Test
  public void testDeleteVolume()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    assertNotNull(volume);
    store.deleteVolume(volumeName);
    OzoneTestUtils.expectOmException(ResultCodes.VOLUME_NOT_FOUND,
        () -> store.getVolume(volumeName));

  }

  @Test
  public void testCreateVolumeWithMetadata()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .addMetadata("key1", "val1")
        .build();
    store.createVolume(volumeName, volumeArgs);
    OzoneVolume volume = store.getVolume(volumeName);
    assertEquals(OzoneConsts.QUOTA_RESET, volume.getQuotaInNamespace());
    assertEquals(OzoneConsts.QUOTA_RESET, volume.getQuotaInBytes());
    assertEquals("val1", volume.getMetadata().get("key1"));
    assertEquals(volumeName, volume.getName());
  }

  @Test
  public void testCreateBucketWithMetadata()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    assertEquals(OzoneConsts.QUOTA_RESET, volume.getQuotaInNamespace());
    assertEquals(OzoneConsts.QUOTA_RESET, volume.getQuotaInBytes());
    BucketArgs args = BucketArgs.newBuilder()
        .addMetadata("key1", "value1").build();
    volume.createBucket(bucketName, args);
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertEquals(bucketName, bucket.getName());
    assertEquals(OzoneConsts.QUOTA_RESET, bucket.getQuotaInNamespace());
    assertEquals(OzoneConsts.QUOTA_RESET, bucket.getQuotaInBytes());
    assertNotNull(bucket.getMetadata());
    assertEquals("value1", bucket.getMetadata().get("key1"));

  }

  @Test
  public void testCreateBucket()
      throws IOException {
    Instant testStartTime = getTestStartTime();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertEquals(bucketName, bucket.getName());
    assertFalse(bucket.getCreationTime().isBefore(testStartTime));
    assertFalse(volume.getCreationTime().isBefore(testStartTime));
  }

  @Test
  public void testCreateS3Bucket()
      throws IOException {
    Instant testStartTime = getTestStartTime();
    String bucketName = UUID.randomUUID().toString();
    store.createS3Bucket(bucketName);
    OzoneBucket bucket = store.getS3Bucket(bucketName);
    assertEquals(bucketName, bucket.getName());
    assertFalse(bucket.getCreationTime().isBefore(testStartTime));
    assertEquals(BucketLayout.OBJECT_STORE, bucket.getBucketLayout());
  }

  @Test
  public void testDeleteS3Bucket()
      throws Exception {
    Instant testStartTime = getTestStartTime();
    String bucketName = UUID.randomUUID().toString();
    store.createS3Bucket(bucketName);
    OzoneBucket bucket = store.getS3Bucket(bucketName);
    assertEquals(bucketName, bucket.getName());
    assertFalse(bucket.getCreationTime().isBefore(testStartTime));
    store.deleteS3Bucket(bucketName);

    OzoneTestUtils.expectOmException(ResultCodes.BUCKET_NOT_FOUND,
        () -> store.getS3Bucket(bucketName));
  }

  @Test
  public void testDeleteS3NonExistingBucket() {
    OMException omException = assertThrows(OMException.class, () -> store.deleteS3Bucket(UUID.randomUUID().toString()));
    assertSame(ResultCodes.BUCKET_NOT_FOUND, omException.getResult());
    assertThat(omException).hasMessage("Bucket not found");
  }

  @Test
  public void testCreateBucketWithVersioning()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setVersioning(true);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertEquals(bucketName, bucket.getName());
    assertTrue(bucket.getVersioning());
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
    assertEquals(bucketName, bucket.getName());
    assertEquals(StorageType.SSD, bucket.getStorageType());
  }

  @Test
  public void testCreateBucketWithAcls()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = OzoneAcl.of(USER, "test",
        ACCESS, READ);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder()
        .addAcl(userAcl);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertEquals(bucketName, bucket.getName());
    assertThat(bucket.getAcls()).contains(userAcl);
  }

  @Test
  public void testCreateBucketWithReplicationConfig()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    ReplicationConfig repConfig = new ECReplicationConfig(3, 2);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setDefaultReplicationConfig(new DefaultReplicationConfig(repConfig))
        .build();
    volume.createBucket(bucketName, bucketArgs);
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertEquals(bucketName, bucket.getName());
    assertEquals(repConfig, bucket.getReplicationConfig());
  }

  @Test
  public void testCreateBucketWithAllArgument()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = OzoneAcl.of(USER, "test",
        ACCESS, ALL);
    ReplicationConfig repConfig = new ECReplicationConfig(3, 2);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setVersioning(true)
        .setStorageType(StorageType.SSD)
        .addAcl(userAcl)
        .setDefaultReplicationConfig(new DefaultReplicationConfig(repConfig));
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertEquals(bucketName, bucket.getName());
    assertTrue(bucket.getVersioning());
    assertEquals(StorageType.SSD, bucket.getStorageType());
    assertThat(bucket.getAcls()).contains(userAcl);
    assertEquals(repConfig, bucket.getReplicationConfig());
  }

  @Test
  public void testInvalidBucketCreation() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = "invalid#bucket";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    OMException omException = assertThrows(OMException.class,
        () -> volume.createBucket(bucketName));
    assertEquals("bucket name has an unsupported character : #",
        omException.getMessage());
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
    acls.add(OzoneAcl.of(USER, "test", ACCESS, ALL));
    OzoneBucket bucket = volume.getBucket(bucketName);
    for (OzoneAcl acl : acls) {
      assertTrue(bucket.addAcl(acl));
    }
    OzoneBucket newBucket = volume.getBucket(bucketName);
    assertEquals(bucketName, newBucket.getName());
    assertThat(bucket.getAcls()).contains(acls.get(0));
  }

  @Test
  public void testRemoveBucketAcl()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = OzoneAcl.of(USER, "test",
        ACCESS, ALL);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder()
        .addAcl(userAcl);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertTrue(bucket.removeAcl(userAcl));
    OzoneBucket newBucket = volume.getBucket(bucketName);
    assertEquals(bucketName, newBucket.getName());
    assertThat(newBucket.getAcls()).doesNotContain(userAcl);
  }

  @Test
  public void testRemoveBucketAclUsingRpcClientRemoveAcl()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = OzoneAcl.of(USER, "test",
        ACCESS, ALL);
    OzoneAcl acl2 = OzoneAcl.of(USER, "test1",
        ACCESS, ALL);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder()
        .addAcl(userAcl)
        .addAcl(acl2);
    volume.createBucket(bucketName, builder.build());
    OzoneObj ozoneObj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setResType(OzoneObj.ResourceType.BUCKET).build();

    // Remove the 2nd acl added to the list.
    assertTrue(store.removeAcl(ozoneObj, acl2));
    assertThat(store.getAcl(ozoneObj)).doesNotContain(acl2);

    assertTrue(store.removeAcl(ozoneObj, userAcl));
    assertThat(store.getAcl(ozoneObj)).doesNotContain(userAcl);
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
    assertEquals(bucketName, newBucket.getName());
    assertTrue(newBucket.getVersioning());
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
    assertEquals(bucketName, newBucket.getName());
    assertTrue(newBucket.getVersioning());

    List<OzoneAcl> aclsAfterSet = newBucket.getAcls();
    assertEquals(currentAcls, aclsAfterSet);

  }

  @Test
  public void testAclDeDuplication()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl1 = OzoneAcl.of(USER, "test", DEFAULT, READ);
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    OzoneAcl currentUserAcl = OzoneAcl.of(USER, currentUser.getShortUserName(), ACCESS, ALL);
    OzoneAcl currentUserPrimaryGroupAcl = OzoneAcl.of(GROUP, currentUser.getPrimaryGroupName(), ACCESS, READ, LIST);
    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner(currentUser.getShortUserName())
        .setAdmin(currentUser.getShortUserName())
        .addAcl(userAcl1)
        .addAcl(currentUserAcl)
        .addAcl(currentUserPrimaryGroupAcl)
        .build();

    store.createVolume(volumeName, createVolumeArgs);
    OzoneVolume volume = store.getVolume(volumeName);
    List<OzoneAcl> volumeAcls = volume.getAcls();
    assertEquals(3, volumeAcls.size());
    assertTrue(volumeAcls.contains(userAcl1));
    assertTrue(volumeAcls.contains(currentUserAcl));
    assertTrue(volumeAcls.contains(currentUserPrimaryGroupAcl));

    // normal bucket
    BucketArgs.Builder builder = BucketArgs.newBuilder()
        .addAcl(currentUserAcl).addAcl(currentUserPrimaryGroupAcl);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    List<OzoneAcl> bucketAcls = bucket.getAcls();
    assertEquals(bucketName, bucket.getName());
    assertEquals(3, bucketAcls.size());
    assertTrue(bucketAcls.contains(currentUserAcl));
    assertTrue(bucketAcls.contains(currentUserPrimaryGroupAcl));
    assertEquals(userAcl1.getName(), bucketAcls.get(2).getName());
    assertEquals(userAcl1.getAclList(), bucketAcls.get(2).getAclList());
    assertEquals(ACCESS, bucketAcls.get(2).getAclScope());

    // link bucket
    OzoneAcl userAcl2 = OzoneAcl.of(USER, "test-link", DEFAULT, READ);
    String linkBucketName =  "link-" + bucketName;
    builder = BucketArgs.newBuilder().setSourceVolume(volumeName).setSourceBucket(bucketName)
        .addAcl(currentUserAcl).addAcl(currentUserPrimaryGroupAcl).addAcl(userAcl2);
    volume.createBucket(linkBucketName, builder.build());
    OzoneBucket linkBucket = volume.getBucket(linkBucketName);
    List<OzoneAcl> linkBucketAcls = linkBucket.getAcls();
    assertEquals(linkBucketName, linkBucket.getName());
    assertEquals(5, linkBucketAcls.size());
    assertTrue(linkBucketAcls.contains(currentUserAcl));
    assertTrue(linkBucketAcls.contains(currentUserPrimaryGroupAcl));
    assertTrue(linkBucketAcls.contains(userAcl2));
    assertTrue(linkBucketAcls.contains(OzoneAcl.LINK_BUCKET_DEFAULT_ACL));
    assertEquals(userAcl1.getName(), linkBucketAcls.get(4).getName());
    assertEquals(userAcl1.getAclList(), linkBucketAcls.get(4).getAclList());
    assertEquals(ACCESS, linkBucketAcls.get(4).getAclScope());
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
    assertEquals(bucketName, newBucket.getName());
    assertEquals(StorageType.SSD, newBucket.getStorageType());
  }

  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testDeleteBucketWithTheSamePrefix(BucketLayout bucketLayout)
      throws Exception {
    /*
    * This test case simulates the following operations:
    * 1. (op1) Create a bucket named "bucket1".
    * 2. (op2) Create another bucket named "bucket11",
    *          which has the same prefix as "bucket1".
    * 3. (op3) Put a key in the "bucket11".
    * 4. (op4) Delete the "bucket1". This operation should succeed.
    * */
    String volumeName = UUID.randomUUID().toString();
    String bucketName1 = "bucket1";
    String bucketName2 = "bucket11";
    String keyName = "key";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs bucketArgs =
        BucketArgs.newBuilder().setBucketLayout(bucketLayout).build();
    volume.createBucket(bucketName1, bucketArgs);
    volume.createBucket(bucketName2, bucketArgs);
    OzoneBucket bucket2 = volume.getBucket(bucketName2);
    bucket2.createKey(keyName, 1).close();
    volume.deleteBucket(bucketName1);
    OzoneTestUtils.expectOmException(ResultCodes.BUCKET_NOT_FOUND,
        () -> volume.getBucket(bucketName1));
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
    assertNotNull(bucket);
    volume.deleteBucket(bucketName);

    OzoneTestUtils.expectOmException(ResultCodes.BUCKET_NOT_FOUND,
        () -> volume.getBucket(bucketName)
    );
  }

  @Test
  public void testDeleteLinkedBucket() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String linkedBucketName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);

    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertNotNull(bucket);

    volume.createBucket(linkedBucketName,
        BucketArgs.newBuilder()
            .setSourceBucket(bucketName)
            .setSourceVolume(volumeName)
            .build());
    OzoneBucket linkedBucket = volume.getBucket(linkedBucketName);
    assertNotNull(linkedBucket);

    volume.deleteBucket(bucketName);

    OzoneTestUtils.expectOmException(ResultCodes.BUCKET_NOT_FOUND,
        () -> volume.getBucket(bucketName)
    );
    //now linkedBucketName has become a dangling one
    //should still be possible to get its info
    OzoneBucket danglingLinkedBucket = volume.getBucket(linkedBucketName);
    assertNotNull(danglingLinkedBucket);

    //now delete the dangling linked bucket
    volume.deleteBucket(linkedBucketName);

    OzoneTestUtils.expectOmException(ResultCodes.BUCKET_NOT_FOUND,
        () -> volume.getBucket(linkedBucketName)
    );

    store.deleteVolume(volumeName);
  }

  @Test
  public void testDeleteAuditLog() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    byte[] value = "sample value".getBytes(UTF_8);
    int valueLength = value.length;
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    // create a three replica file
    String keyName1 = "key1";
    TestDataUtil.createKey(bucket, keyName1, ReplicationConfig
        .fromTypeAndFactor(RATIS, THREE), value);

    // create a EC replica file
    String keyName2 = "key2";
    ReplicationConfig replicationConfig = new ECReplicationConfig("rs-3-2-1024k");
    TestDataUtil.createKey(bucket, keyName2, replicationConfig, value);

    // create a directory and a file
    String dirName = "dir1";
    bucket.createDirectory(dirName);
    String keyName3 = "key3";
    TestDataUtil.createKey(bucket, keyName3, ReplicationConfig
        .fromTypeAndFactor(RATIS, THREE), value);

    // delete files and directory
    output.reset();
    bucket.deleteKey(keyName1);
    bucket.deleteKey(keyName2);
    bucket.deleteDirectory(dirName, true);

    // create keys for deleteKeys case
    String keyName4 = "key4";
    TestDataUtil.createKey(bucket, dirName + "/" + keyName4,
        ReplicationConfig.fromTypeAndFactor(RATIS, THREE), value);

    String keyName5 = "key5";
    TestDataUtil.createKey(bucket, dirName + "/" + keyName5, replicationConfig, value);

    List<String> keysToDelete = new ArrayList<>();
    keysToDelete.add(dirName + "/" + keyName4);
    keysToDelete.add(dirName + "/" + keyName5);
    bucket.deleteKeys(keysToDelete);

    String consoleOutput = output.get();
    assertThat(consoleOutput).contains("op=DELETE_KEY {\"volume\":\"" + volumeName + "\",\"bucket\":\"" + bucketName +
        "\",\"key\":\"key1\",\"dataSize\":\"" + valueLength + "\",\"replicationConfig\":\"RATIS/THREE");
    assertThat(consoleOutput).contains("op=DELETE_KEY {\"volume\":\"" + volumeName + "\",\"bucket\":\"" + bucketName +
        "\",\"key\":\"key2\",\"dataSize\":\"" + valueLength + "\",\"replicationConfig\":\"EC{rs-3-2-1024k}");
    assertThat(consoleOutput).contains("op=DELETE_KEY {\"volume\":\"" + volumeName + "\",\"bucket\":\"" + bucketName +
        "\",\"key\":\"dir1\",\"Transaction\"");
    assertThat(consoleOutput).contains("op=DELETE_KEYS {\"volume\":\"" + volumeName + "\",\"bucket\":\"" + bucketName +
        "\",\"deletedKeysList\":\"{key=dir1/key4, dataSize=" + valueLength +
        ", replicationConfig=RATIS/THREE}, {key=dir1/key5, dataSize=" + valueLength +
        ", replicationConfig=EC{rs-3-2-1024k}}\",\"unDeletedKeysList\"");
  }

  protected void verifyReplication(String volumeName, String bucketName,
      String keyName, ReplicationConfig replication)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
    OmKeyInfo keyInfo = ozoneManager.lookupKey(keyArgs);
    for (OmKeyLocationInfo info:
        keyInfo.getLatestVersionLocations().getLocationList()) {
      ContainerInfo container =
          storageContainerLocationClient.getContainer(info.getContainerID());
      assertEquals(replication, container.getReplicationConfig());
    }
  }

  @ParameterizedTest
  @CsvSource({"rs-3-3-1024k,false", "xor-3-5-2048k,false",
              "rs-3-2-1024k,true", "rs-6-3-1024k,true", "rs-10-4-1024k,true"})
  public void testPutKeyWithReplicationConfig(String replicationValue,
                                              boolean isValidReplicationConfig)
          throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String keyName = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();
    ReplicationConfig replicationConfig =
            new ECReplicationConfig(replicationValue);
    if (isValidReplicationConfig) {
      TestDataUtil.createKey(bucket, keyName,
          replicationConfig, value.getBytes(UTF_8));
      OzoneKey key = bucket.getKey(keyName);
      assertEquals(keyName, key.getName());
      try (OzoneInputStream is = bucket.readKey(keyName)) {
        byte[] fileContent = new byte[value.getBytes(UTF_8).length];
        IOUtils.readFully(is, fileContent);
        assertEquals(value, new String(fileContent, UTF_8));
      }
    } else {
      assertThrows(IllegalArgumentException.class,
          () -> bucket.createKey(keyName, "dummy".getBytes(UTF_8).length,
              replicationConfig, new HashMap<>()));
    }
  }

  @Test
  public void testPutKey() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    Instant testStartTime = getTestStartTime();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      TestDataUtil.createKey(bucket, keyName,
          ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
          value.getBytes(UTF_8));
      OzoneKey key = bucket.getKey(keyName);
      assertEquals(keyName, key.getName());
      try (OzoneInputStream is = bucket.readKey(keyName)) {
        byte[] fileContent = new byte[value.getBytes(UTF_8).length];
        IOUtils.readFully(is, fileContent);
        verifyReplication(volumeName, bucketName, keyName,
            RatisReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE));
        assertEquals(value, new String(fileContent, UTF_8));
        assertFalse(key.getCreationTime().isBefore(testStartTime));
        assertFalse(key.getModificationTime().isBefore(testStartTime));
      }
    }
  }

  @ParameterizedTest
  @EnumSource
  void rewriteKey(BucketLayout layout) throws IOException {
    checkFeatureEnable(OzoneManagerVersion.ATOMIC_REWRITE_KEY);
    OzoneBucket bucket = createBucket(layout);
    OzoneKeyDetails keyDetails = createTestKey(bucket);
    OmKeyArgs keyArgs = toOmKeyArgs(keyDetails);
    OmKeyInfo keyInfo = ozoneManager.lookupKey(keyArgs);

    final byte[] newContent = "rewrite value".getBytes(UTF_8);
    rewriteKey(bucket, keyDetails, newContent);

    OzoneKeyDetails actualKeyDetails = assertKeyContent(bucket, keyDetails.getName(), newContent);
    assertThat(actualKeyDetails.getGeneration()).isGreaterThan(keyDetails.getGeneration());
    assertMetadataUnchanged(keyDetails, actualKeyDetails);
    assertMetadataAfterRewrite(keyInfo, ozoneManager.lookupKey(keyArgs));
  }

  @ParameterizedTest
  @EnumSource
  void overwriteAfterRewrite(BucketLayout layout) throws IOException {
    checkFeatureEnable(OzoneManagerVersion.ATOMIC_REWRITE_KEY);
    OzoneBucket bucket = createBucket(layout);
    OzoneKeyDetails keyDetails = createTestKey(bucket);
    rewriteKey(bucket, keyDetails, "rewrite".getBytes(UTF_8));

    final byte[] overwriteContent = "overwrite".getBytes(UTF_8);
    OzoneKeyDetails overwriteDetails = createTestKey(bucket, keyDetails.getName(), overwriteContent);

    OzoneKeyDetails actualKeyDetails = assertKeyContent(bucket, keyDetails.getName(), overwriteContent);
    assertEquals(overwriteDetails.getGeneration(), actualKeyDetails.getGeneration());
  }

  @ParameterizedTest
  @EnumSource
  void rewriteAfterRename(BucketLayout layout) throws IOException {
    checkFeatureEnable(OzoneManagerVersion.ATOMIC_REWRITE_KEY);
    OzoneBucket bucket = createBucket(layout);
    OzoneKeyDetails keyDetails = createTestKey(bucket);
    String newKeyName = "rewriteAfterRename-" + layout;

    bucket.renameKey(keyDetails.getName(), newKeyName);
    OzoneKeyDetails renamedKeyDetails = bucket.getKey(newKeyName);
    OmKeyArgs keyArgs = toOmKeyArgs(renamedKeyDetails);
    OmKeyInfo keyInfo = ozoneManager.lookupKey(keyArgs);

    final byte[] rewriteContent = "rewrite".getBytes(UTF_8);
    rewriteKey(bucket, renamedKeyDetails, rewriteContent);

    OzoneKeyDetails actualKeyDetails = assertKeyContent(bucket, newKeyName, rewriteContent);
    assertMetadataUnchanged(keyDetails, actualKeyDetails);
    assertMetadataAfterRewrite(keyInfo, ozoneManager.lookupKey(keyArgs));
  }

  @ParameterizedTest
  @EnumSource
  void renameAfterRewrite(BucketLayout layout) throws IOException {
    checkFeatureEnable(OzoneManagerVersion.ATOMIC_REWRITE_KEY);
    OzoneBucket bucket = createBucket(layout);
    OzoneKeyDetails keyDetails = createTestKey(bucket);
    final byte[] rewriteContent = "rewrite".getBytes(UTF_8);
    rewriteKey(bucket, keyDetails, rewriteContent);
    OmKeyInfo keyInfo = ozoneManager.lookupKey(toOmKeyArgs(keyDetails));

    String newKeyName = "renameAfterRewrite-" + layout;
    bucket.renameKey(keyDetails.getName(), newKeyName);

    OzoneKeyDetails actualKeyDetails = assertKeyContent(bucket, newKeyName, rewriteContent);
    assertMetadataUnchanged(keyDetails, actualKeyDetails);
    assertMetadataAfterRewrite(keyInfo, ozoneManager.lookupKey(toOmKeyArgs(actualKeyDetails)));
  }

  @ParameterizedTest
  @EnumSource
  void rewriteFailsDueToOutdatedGeneration(BucketLayout layout) throws IOException {
    checkFeatureEnable(OzoneManagerVersion.ATOMIC_REWRITE_KEY);
    OzoneBucket bucket = createBucket(layout);
    OzoneKeyDetails keyDetails = createTestKey(bucket);
    OmKeyArgs keyArgs = toOmKeyArgs(keyDetails);

    // overwrite to get new generation
    final byte[] overwriteContent = "overwrite".getBytes(UTF_8);
    OzoneKeyDetails overwriteDetails = createTestKey(bucket, keyDetails.getName(), overwriteContent);
    OmKeyInfo keyInfo = ozoneManager.lookupKey(keyArgs);

    // try to rewrite previous generation
    OMException e = assertThrows(OMException.class, () -> rewriteKey(bucket, keyDetails, "rewrite".getBytes(UTF_8)));
    assertEquals(KEY_NOT_FOUND, e.getResult());
    assertThat(e).hasMessageContaining("Generation mismatch");

    OzoneKeyDetails actualKeyDetails = assertKeyContent(bucket, keyDetails.getName(), overwriteContent);
    assertEquals(overwriteDetails.getGeneration(), actualKeyDetails.getGeneration());
    assertMetadataUnchanged(overwriteDetails, actualKeyDetails);
    assertUnchanged(keyInfo, ozoneManager.lookupKey(keyArgs));
  }

  @ParameterizedTest
  @EnumSource
  void rewriteFailsDueToOutdatedGenerationAtCommit(BucketLayout layout) throws IOException {
    checkFeatureEnable(OzoneManagerVersion.ATOMIC_REWRITE_KEY);
    OzoneBucket bucket = createBucket(layout);
    OzoneKeyDetails keyDetails = createTestKey(bucket);
    final byte[] overwriteContent = "overwrite".getBytes(UTF_8);
    OmKeyArgs keyArgs = toOmKeyArgs(keyDetails);
    OmKeyInfo keyInfo;

    OzoneOutputStream out = null;
    final OzoneKeyDetails overwriteDetails;
    try {
      out = bucket.rewriteKey(keyDetails.getName(), keyDetails.getDataSize(),
          keyDetails.getGeneration(), RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
          keyDetails.getMetadata());
      out.write("rewrite".getBytes(UTF_8));

      overwriteDetails = createTestKey(bucket, keyDetails.getName(), overwriteContent);
      keyInfo = ozoneManager.lookupKey(keyArgs);

      OMException e = assertThrows(OMException.class, out::close);
      assertEquals(KEY_GENERATION_MISMATCH, e.getResult());
      assertThat(e).hasMessageContaining("does not match the expected generation to rewrite");
    } finally {
      if (out != null) {
        out.close();
      }
    }

    OzoneKeyDetails actualKeyDetails = assertKeyContent(bucket, keyDetails.getName(), overwriteContent);
    assertEquals(overwriteDetails.getGeneration(), actualKeyDetails.getGeneration());
    assertUnchanged(keyInfo, ozoneManager.lookupKey(keyArgs));
  }

  @ParameterizedTest
  @EnumSource
  void cannotRewriteDeletedKey(BucketLayout layout) throws IOException {
    checkFeatureEnable(OzoneManagerVersion.ATOMIC_REWRITE_KEY);
    OzoneBucket bucket = createBucket(layout);
    OzoneKeyDetails keyDetails = createTestKey(bucket);
    bucket.deleteKey(keyDetails.getName());

    OMException e = assertThrows(OMException.class, () -> rewriteKey(bucket, keyDetails, "rewrite".getBytes(UTF_8)));
    assertEquals(KEY_NOT_FOUND, e.getResult());
    assertThat(e).hasMessageContaining("not found");
  }

  @ParameterizedTest
  @EnumSource
  void cannotRewriteRenamedKey(BucketLayout layout) throws IOException {
    checkFeatureEnable(OzoneManagerVersion.ATOMIC_REWRITE_KEY);
    OzoneBucket bucket = createBucket(layout);
    OzoneKeyDetails keyDetails = createTestKey(bucket);
    bucket.renameKey(keyDetails.getName(), "newKeyName-" + layout.name());

    OMException e = assertThrows(OMException.class, () -> rewriteKey(bucket, keyDetails, "rewrite".getBytes(UTF_8)));
    assertEquals(KEY_NOT_FOUND, e.getResult());
    assertThat(e).hasMessageContaining("not found");
  }

  private static void rewriteKey(
      OzoneBucket bucket, OzoneKeyDetails keyDetails, byte[] newContent
  ) throws IOException {
    try (OzoneOutputStream out = bucket.rewriteKey(keyDetails.getName(), keyDetails.getDataSize(),
        keyDetails.getGeneration(), RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        keyDetails.getMetadata())) {
      out.write(newContent);
    }
  }

  private OzoneBucket createBucket(BucketLayout layout) throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);

    BucketArgs args = BucketArgs.newBuilder()
        .setBucketLayout(layout)
        .build();

    volume.createBucket(bucketName, args);
    return volume.getBucket(bucketName);
  }

  private static OmKeyArgs toOmKeyArgs(OzoneKeyDetails keyDetails) {
    return new OmKeyArgs.Builder()
        .setVolumeName(keyDetails.getVolumeName())
        .setBucketName(keyDetails.getBucketName())
        .setKeyName(keyDetails.getName())
        .build();
  }

  private static void assertUnchanged(OmKeyInfo original, OmKeyInfo current) {
    assertEquals(original.getAcls(), current.getAcls());
    assertEquals(original.getMetadata(), current.getMetadata());
    assertEquals(original.getCreationTime(), current.getCreationTime());
    assertEquals(original.getUpdateID(), current.getUpdateID());
    assertEquals(original.getModificationTime(), current.getModificationTime());
  }

  private static void assertMetadataAfterRewrite(OmKeyInfo original, OmKeyInfo updated) {
    assertEquals(original.getAcls(), updated.getAcls());
    assertEquals(original.getMetadata(), updated.getMetadata());
    assertEquals(original.getCreationTime(), updated.getCreationTime());
    assertThat(updated.getUpdateID()).isGreaterThan(original.getUpdateID());
    assertThat(updated.getModificationTime()).isGreaterThanOrEqualTo(original.getModificationTime());
  }

  private static void assertMetadataUnchanged(OzoneKeyDetails original, OzoneKeyDetails rewritten) {
    assertEquals(original.getOwner(), rewritten.getOwner());
    assertEquals(original.getMetadata(), rewritten.getMetadata());
  }

  private static void checkFeatureEnable(OzoneManagerVersion feature) {
    try {
      cluster.getOzoneManager().checkFeatureEnabled(feature);
    } catch (OMException e) {
      assumeFalse(OMException.ResultCodes.NOT_SUPPORTED_OPERATION == e.getResult());
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

    store.createVolume(volumeName);
    volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    final OzoneBucket bucket = volume.getBucket(bucketName);
    bucket.setQuota(OzoneQuota.parseQuota("1 B", "100"));
    store.getVolume(volumeName).setQuota(
        OzoneQuota.parseQuota(Long.MAX_VALUE + " B", "100"));

    // Test bucket quota: write key.
    // The remaining quota does not satisfy a block size, so the writing fails.

    OMException omException = assertThrows(OMException.class,
        () -> writeKey(bucket, UUID.randomUUID().toString(), ONE, value, valueLength));
    assertSame(ResultCodes.QUOTA_EXCEEDED, omException.getResult());
    // Write failed, bucket usedBytes should be 0
    assertEquals(0L,
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());

    // Test bucket quota: write file.
    // The remaining quota does not satisfy a block size, so the write fails.

    omException = assertThrows(OMException.class,
        () -> writeFile(bucket, UUID.randomUUID().toString(), ONE, value, 0));
    assertSame(ResultCodes.QUOTA_EXCEEDED, omException.getResult());
    // Write failed, bucket usedBytes should be 0
    assertEquals(0L, store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());

    // Test bucket quota: write large key(with five blocks), the first four
    // blocks will succeed，while the later block will fail.
    bucket.setQuota(OzoneQuota.parseQuota(
        4 * blockSize + " B", "100"));

    IOException ioException = assertThrows(IOException.class, () -> {
      String keyName = UUID.randomUUID().toString();
      try (OzoneOutputStream out = bucket.createKey(keyName, valueLength, RATIS, ONE, new HashMap<>())) {
        for (int i = 0; i <= (4 * blockSize) / value.length(); i++) {
          out.write(value.getBytes(UTF_8));
        }
      }
    });
    assertThat(ioException).hasCauseInstanceOf(OMException.class).hasMessageContaining("QUOTA_EXCEEDED");

    // AllocateBlock failed, bucket usedBytes should not update.
    assertEquals(0L, store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());

    // Reset bucket quota, the original usedBytes needs to remain the same
    bucket.setQuota(OzoneQuota.parseQuota(
        100 + " GB", "100"));
    assertEquals(0,
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());

    // key with 0 bytes, usedBytes should not increase.
    bucket.setQuota(OzoneQuota.parseQuota(
        5 * blockSize + " B", "100"));
    OzoneOutputStream out = bucket.createKey(UUID.randomUUID().toString(),
        valueLength, RATIS, ONE, new HashMap<>());
    out.close();
    assertEquals(0,
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());

    // key write success,bucket usedBytes should update.
    bucket.setQuota(OzoneQuota.parseQuota(
        5 * blockSize + " B", "100"));
    OzoneOutputStream out2 = bucket.createKey(UUID.randomUUID().toString(),
        valueLength, RATIS, ONE, new HashMap<>());
    out2.write(value.getBytes(UTF_8));
    out2.close();
    assertEquals(valueLength,
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());
  }

  @Test
  public void testBucketUsedBytes() throws IOException, InterruptedException, TimeoutException {
    bucketUsedBytesTestHelper(BucketLayout.OBJECT_STORE);
  }

  @Test
  public void testFSOBucketUsedBytes() throws IOException, InterruptedException, TimeoutException {
    bucketUsedBytesTestHelper(BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  private void bucketUsedBytesTestHelper(BucketLayout bucketLayout)
      throws IOException, InterruptedException, TimeoutException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    int blockSize = (int) ozoneManager.getConfiguration().getStorageSize(
        OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);
    OzoneVolume volume = null;
    String value = "sample value";
    int valueLength = value.getBytes(UTF_8).length;
    store.createVolume(volumeName);
    volume = store.getVolume(volumeName);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setBucketLayout(bucketLayout).build();
    volume.createBucket(bucketName, bucketArgs);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();

    writeKey(bucket, keyName, ONE, value, valueLength);
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> valueLength ==
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes(), 1000, 30000);

    writeKey(bucket, keyName, ONE, value, valueLength);
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> valueLength ==
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes(), 1000, 30000);

    // pre-allocate more blocks than needed
    int fakeValueLength = valueLength + blockSize;
    writeKey(bucket, keyName, ONE, value, fakeValueLength);
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> valueLength ==
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes(), 1000, 30000);

    bucket.deleteKey(keyName);
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> 0L ==
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes(), 1000, 30000);
  }

  static Stream<BucketLayout> bucketLayouts() {
    return Stream.of(
        BucketLayout.OBJECT_STORE,
        BucketLayout.LEGACY,
        BucketLayout.FILE_SYSTEM_OPTIMIZED
    );
  }

  static Stream<Arguments> bucketLayoutsWithEnablePaths() {
    return bucketLayouts()
        .flatMap(layout -> Stream.of(
            Arguments.of(layout, true),
            Arguments.of(layout, false)
        ));
  }

  @ParameterizedTest
  @MethodSource("bucketLayouts")
  void bucketUsedBytesOverWrite(BucketLayout bucketLayout)
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneVolume volume = null;
    String value = "sample value";
    int valueLength = value.getBytes(UTF_8).length;
    store.createVolume(volumeName);
    volume = store.getVolume(volumeName);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setBucketLayout(bucketLayout).setVersioning(true).build();
    volume.createBucket(bucketName, bucketArgs);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();

    writeKey(bucket, keyName, ONE, value, valueLength);
    assertEquals(valueLength,
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());

    // Overwrite the same key, because this bucket setVersioning is true,
    // so the bucket usedBytes should increase.
    writeKey(bucket, keyName, ONE, value, valueLength);
    assertEquals(valueLength * 2,
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes());
  }


  // TODO: testBucketQuota overlaps with testBucketUsedBytes,
  //       do cleanup when EC branch gets merged into master.
  @ParameterizedTest
  @MethodSource("replicationConfigs")
  void testBucketQuota(ReplicationConfig repConfig) throws IOException, InterruptedException, TimeoutException {
    int blockSize = (int) ozoneManager.getConfiguration().getStorageSize(
        OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);

    for (int i = 0; i <= repConfig.getRequiredNodes(); i++) {
      bucketQuotaTestHelper(i * blockSize, repConfig);
      bucketQuotaTestHelper(i * blockSize + 1, repConfig);
    }
  }

  private void bucketQuotaTestHelper(int keyLength, ReplicationConfig repConfig)
      throws IOException, InterruptedException, TimeoutException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    byte[] value = new byte[keyLength];
    long keyQuota = QuotaUtil.getReplicatedSize(keyLength, repConfig);

    OzoneOutputStream out = bucket.createKey(keyName, keyLength,
        repConfig, new HashMap<>());
    // Write a new key and do not update Bucket UsedBytes until commit.
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> 0 ==
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes(), 1000, 30000);
    out.write(value);
    out.close();
    // After committing the new key, the Bucket UsedBytes must be updated to
    // keyQuota.
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> keyQuota ==
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes(), 1000, 30000);

    out = bucket.createKey(keyName, keyLength, repConfig, new HashMap<>());
    // Overwrite an old key. The Bucket UsedBytes are not updated before the
    // commit. So the Bucket UsedBytes remain unchanged.
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> keyQuota ==
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes(), 1000, 30000);
    out.write(value);
    out.close();
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> keyQuota ==
        store.getVolume(volumeName).getBucket(bucketName).getUsedBytes(), 1000, 30000);

    bucket.deleteKey(keyName);
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> 0L == store.getVolume(volumeName)
        .getBucket(bucketName).getUsedBytes(), 1000, 30000);
  }

  @Flaky("HDDS-13879")
  @ParameterizedTest
  @MethodSource("bucketLayoutsWithEnablePaths")
  public void testBucketUsedNamespace(BucketLayout layout, boolean enablePaths)
      throws IOException, InterruptedException, TimeoutException {
    boolean originalEnablePaths = cluster.getOzoneManager().getConfig().isFileSystemPathEnabled();
    cluster.getOzoneManager().getConfig().setFileSystemPathEnabled(enablePaths);
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String value = "sample value";
    int valueLength = value.getBytes(UTF_8).length;
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setBucketLayout(layout)
        .build();
    volume.createBucket(bucketName, bucketArgs);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName1 = UUID.randomUUID().toString();
    String keyName2 = UUID.randomUUID().toString();

    writeKey(bucket, keyName1, ONE, value, valueLength);
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> 1L == getBucketUsedNamespace(volumeName,
        bucketName), 1000, 30000);
    // Test create a file twice will not increase usedNamespace twice
    writeKey(bucket, keyName1, ONE, value, valueLength);
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> 1L == getBucketUsedNamespace(volumeName,
        bucketName), 1000, 30000);
    writeKey(bucket, keyName2, ONE, value, valueLength);
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> 2L == getBucketUsedNamespace(volumeName,
        bucketName), 1000, 30000);
    bucket.deleteKey(keyName1);
    GenericTestUtils.waitFor(
        (CheckedSupplier<Boolean, IOException>) () -> 1L == getBucketUsedNamespace(volumeName, bucketName),
        1000, 30000);
    bucket.deleteKey(keyName2);
    GenericTestUtils.waitFor(
        (CheckedSupplier<Boolean, IOException>) () -> 0L == getBucketUsedNamespace(volumeName, bucketName),
        1000, 30000);

    RpcClient client = new RpcClient(cluster.getConf(), null);
    try {
      String directoryName1 = UUID.randomUUID().toString();
      String directoryName2 = UUID.randomUUID().toString();

      client.createDirectory(volumeName, bucketName, directoryName1);
      GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> 1L == getBucketUsedNamespace(volumeName,
          bucketName), 1000, 30000);
      // Test create a directory twice will not increase usedNamespace twice
      client.createDirectory(volumeName, bucketName, directoryName2);
      GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> 2L == getBucketUsedNamespace(volumeName,
          bucketName), 1000, 30000);

      if (layout == BucketLayout.LEGACY) {
        handleLegacyBucketDelete(volumeName, bucketName, directoryName1, directoryName2);
      } else {
        handleNonLegacyBucketDelete(client, volumeName, bucketName, directoryName1, directoryName2);
      }

      String multiComponentsDir = "dir1/dir2/dir3/dir4";
      client.createDirectory(volumeName, bucketName, multiComponentsDir);
      assertEquals(OzoneFSUtils.getFileCount(multiComponentsDir),
          getBucketUsedNamespace(volumeName, bucketName));
    } finally {
      client.close();
      cluster.getOzoneManager().getConfig().setFileSystemPathEnabled(originalEnablePaths);
    }
  }

  private void handleLegacyBucketDelete(String volumeName, String bucketName, String dir1, String dir2)
      throws IOException, InterruptedException, TimeoutException {
    String rootPath = String.format("%s://%s.%s/", OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);
    cluster.getConf().set(FS_DEFAULT_NAME_KEY, rootPath);
    FileSystem fs = FileSystem.get(cluster.getConf());

    org.apache.hadoop.fs.Path dir1Path = new org.apache.hadoop.fs.Path(OZONE_URI_DELIMITER, dir1);
    org.apache.hadoop.fs.Path dir2Path = new org.apache.hadoop.fs.Path(OZONE_URI_DELIMITER, dir2);

    fs.delete(dir1Path, false);
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> 1L == getBucketUsedNamespace(volumeName,
        bucketName), 1000, 30000);
    fs.delete(dir2Path, false);
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> 0L == getBucketUsedNamespace(volumeName,
        bucketName), 1000, 30000);
  }

  private void handleNonLegacyBucketDelete(RpcClient client, String volumeName, String bucketName, String dir1,
      String dir2) throws IOException, InterruptedException, TimeoutException {
    client.deleteKey(volumeName, bucketName, OzoneFSUtils.addTrailingSlashIfNeeded(dir1), false);
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> 1L == getBucketUsedNamespace(volumeName,
        bucketName), 1000, 30000);
    client.deleteKey(volumeName, bucketName, OzoneFSUtils.addTrailingSlashIfNeeded(dir2), false);
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> 0L == getBucketUsedNamespace(volumeName,
        bucketName), 1000, 30000);
  }

  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testMissingParentBucketUsedNamespace(BucketLayout layout)
      throws IOException {
    // when will put a key that contain not exist directory only FSO buckets
    // and LEGACY buckets with ozone.om.enable.filesystem.paths set to true
    // will create missing directories.
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String value = "sample value";
    int valueLength = value.getBytes(UTF_8).length;
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setBucketLayout(layout)
        .build();
    volume.createBucket(bucketName, bucketArgs);
    OzoneBucket bucket = volume.getBucket(bucketName);

    if (layout.equals(BucketLayout.LEGACY)) {
      cluster.getOzoneManager().getConfig().setFileSystemPathEnabled(true);
    }

    // the directory "/dir1", ""/dir1/dir2/", "/dir1/dir2/dir3/"
    // will be created automatically
    String missingParentKeyName = "dir1/dir2/dir3/file1";
    writeKey(bucket, missingParentKeyName, ONE, value, valueLength);
    if (layout.equals(BucketLayout.OBJECT_STORE)) {
      // for OBJECT_STORE bucket, missing parent will not be
      // created automatically
      assertEquals(1, getBucketUsedNamespace(volumeName, bucketName));
    } else {
      assertEquals(OzoneFSUtils.getFileCount(missingParentKeyName),
          getBucketUsedNamespace(volumeName, bucketName));
    }
  }

  private long getBucketUsedNamespace(String volume, String bucket)
      throws IOException {
    return store.getVolume(volume).getBucket(bucket).getUsedNamespace();
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
    assertEquals(0L, volume.getUsedNamespace());
    volume.createBucket(bucketName);
    // Used namespace should be 1
    volume = store.getVolume(volumeName);
    assertEquals(1L, volume.getUsedNamespace());

    OzoneVolume finalVolume = volume;
    OMException omException = assertThrows(OMException.class, () -> finalVolume.createBucket(bucketName2));
    assertEquals(ResultCodes.QUOTA_EXCEEDED, omException.getResult());

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
    assertEquals(0L, volumeWithLinkedBucket.getUsedNamespace());

    // Reset volume quota, the original usedNamespace needs to remain the same
    store.getVolume(volumeName).setQuota(OzoneQuota.parseNameSpaceQuota(
        "100"));
    assertEquals(1L,
        store.getVolume(volumeName).getUsedNamespace());

    volume.deleteBucket(bucketName);
    // Used namespace should be 0
    volume = store.getVolume(volumeName);
    assertEquals(0L, volume.getUsedNamespace());
  }

  @Test
  public void testBucketQuotaInNamespace() throws IOException, InterruptedException, TimeoutException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String key1 = UUID.randomUUID().toString();
    String key2 = UUID.randomUUID().toString();
    String key3 = UUID.randomUUID().toString();

    String value = "sample value";

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    bucket.setQuota(OzoneQuota.parseQuota(Long.MAX_VALUE + " B", "2"));

    writeKey(bucket, key1, ONE, value, value.length());
    assertEquals(1L,
        store.getVolume(volumeName).getBucket(bucketName).getUsedNamespace());

    writeKey(bucket, key2, ONE, value, value.length());
    assertEquals(2L,
        store.getVolume(volumeName).getBucket(bucketName).getUsedNamespace());

    IOException ioException = assertThrows(IOException.class,
        () -> writeKey(bucket, key3, ONE, value, value.length()));
    assertThat(ioException.toString()).contains("QUOTA_EXCEEDED");

    // Write failed, bucket usedNamespace should remain as 2
    assertEquals(2L,
        store.getVolume(volumeName).getBucket(bucketName).getUsedNamespace());

    // Reset bucket quota, the original usedNamespace needs to remain the same
    bucket.setQuota(
        OzoneQuota.parseQuota(Long.MAX_VALUE + " B", "100"));
    assertEquals(2L,
        store.getVolume(volumeName).getBucket(bucketName).getUsedNamespace());

    bucket.deleteKeys(Arrays.asList(key1, key2));
    GenericTestUtils.waitFor((CheckedSupplier<Boolean, IOException>) () -> 0L ==
        store.getVolume(volumeName).getBucket(bucketName).getUsedNamespace(), 1000, 30000);
  }

  private void writeKey(OzoneBucket bucket, String keyName,
                        ReplicationFactor replication, String value,
                        int valueLength)
      throws IOException {
    writeKey(bucket, keyName, replication, value, valueLength,
        Collections.emptyMap(), Collections.emptyMap());
  }

  private void writeKey(OzoneBucket bucket, String keyName,
                        ReplicationFactor replication, String value,
                        int valueLength, Map<String, String> customMetadata,
                        Map<String, String> tags)
      throws IOException {
    OzoneOutputStream out = bucket.createKey(keyName, valueLength,
        ReplicationConfig.fromTypeAndFactor(RATIS, replication), customMetadata, tags);
    out.write(value.getBytes(UTF_8));
    out.close();
  }

  private void writeFile(OzoneBucket bucket, String keyName,
                         ReplicationFactor replication, String value,
                         int valueLength)
      throws IOException {
    OzoneOutputStream out = bucket.createFile(keyName, valueLength, RATIS,
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
        (byte) RandomUtils.secure().randomLong()));
    int valueLength = sampleData.getBytes(UTF_8).length;

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    ReplicationConfig replication = RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.ONE);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        replication);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    assertNotNull(multipartInfo.getUploadID());

    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), 1, uploadID);
    ozoneOutputStream.write(string2Bytes(sampleData), 0,
        sampleData.length());
    ozoneOutputStream.getMetadata().put(ETAG, DigestUtils.md5Hex(sampleData));
    ozoneOutputStream.close();

    assertEquals(valueLength, store.getVolume(volumeName)
        .getBucket(bucketName).getUsedBytes());

    // Abort uploaded partKey and the usedBytes of bucket should be 0.
    bucket.abortMultipartUpload(keyName, uploadID);
    assertEquals(0, store.getVolume(volumeName)
        .getBucket(bucketName).getUsedBytes());
  }

  @Test
  public void testValidateBlockLengthWithCommitKey() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = RandomStringUtils.secure().next(RandomUtils.secure().randomInt(1, 1024));
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();

    // create the initial key with size 0, write will allocate the first block.
    TestDataUtil.createKey(bucket, keyName,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        value.getBytes(UTF_8));
    OmKeyArgs.Builder builder = new OmKeyArgs.Builder();
    builder.setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName);
    OmKeyInfo keyInfo = ozoneManager.lookupKey(builder.build());

    List<OmKeyLocationInfo> locationInfoList =
        keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();
    // LocationList should have only 1 block
    assertEquals(1, locationInfoList.size());
    // make sure the data block size is updated
    assertEquals(value.getBytes(UTF_8).length,
        locationInfoList.get(0).getLength());
    // make sure the total data size is set correctly
    assertEquals(value.getBytes(UTF_8).length, keyInfo.getDataSize());
  }

  @Test
  public void testPutKeyRatisOneNode() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    Instant testStartTime = getTestStartTime();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      TestDataUtil.createKey(bucket, keyName,
          ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
          value.getBytes(UTF_8));
      OzoneKey key = bucket.getKey(keyName);
      assertEquals(keyName, key.getName());
      try (OzoneInputStream is = bucket.readKey(keyName)) {
        byte[] fileContent = new byte[value.getBytes(UTF_8).length];
        IOUtils.readFully(is, fileContent);
        verifyReplication(volumeName, bucketName, keyName,
            RatisReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE));
        assertEquals(value, new String(fileContent, UTF_8));
        assertFalse(key.getCreationTime().isBefore(testStartTime));
        assertFalse(key.getModificationTime().isBefore(testStartTime));
      }
    }
  }

  @Test
  public void testPutKeyRatisThreeNodes() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    Instant testStartTime = getTestStartTime();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      TestDataUtil.createKey(bucket, keyName,
          ReplicationConfig.fromTypeAndFactor(RATIS, THREE),
          value.getBytes(UTF_8));
      OzoneKey key = bucket.getKey(keyName);
      assertEquals(keyName, key.getName());
      try (OzoneInputStream is = bucket.readKey(keyName)) {
        byte[] fileContent = new byte[value.getBytes(UTF_8).length];
        IOUtils.readFully(is, fileContent);
        verifyReplication(volumeName, bucketName, keyName,
            RatisReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.THREE));
        assertEquals(value, new String(fileContent, UTF_8));
        assertFalse(key.getCreationTime().isBefore(testStartTime));
        assertFalse(key.getModificationTime().isBefore(testStartTime));
      }
    }
  }

  @Test
  @Flaky("HDDS-10886")
  public void testPutKeyRatisThreeNodesParallel() throws IOException,
      InterruptedException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    Instant testStartTime = getTestStartTime();
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
              (byte) RandomUtils.secure().randomLong()));
          TestDataUtil.createKey(bucket, keyName,
              ReplicationConfig.fromTypeAndFactor(RATIS, THREE),
              data.getBytes(UTF_8));
          OzoneKey key = bucket.getKey(keyName);
          assertEquals(keyName, key.getName());
          try (OzoneInputStream is = bucket.readKey(keyName)) {
            byte[] fileContent = new byte[data.getBytes(UTF_8).length];
            IOUtils.readFully(is, fileContent);
            verifyReplication(volumeName, bucketName, keyName,
                RatisReplicationConfig.getInstance(
                    HddsProtos.ReplicationFactor.THREE));
            assertEquals(data, new String(fileContent, UTF_8));
          }
          assertFalse(key.getCreationTime().isBefore(testStartTime));
          assertFalse(key.getModificationTime().isBefore(testStartTime));
        }
        latch.countDown();
      } catch (IOException ex) {
        LOG.error("Execution failed: ", ex);
        latch.countDown();
        failCount.incrementAndGet();
      }
    };

    Thread thread1 = new Thread(r);
    Thread thread2 = new Thread(r);

    thread1.start();
    thread2.start();

    assertTrue(latch.await(600, TimeUnit.SECONDS));

    assertThat(failCount.get())
        .withFailMessage("testPutKeyRatisThreeNodesParallel failed")
        .isLessThanOrEqualTo(0);
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
    TestDataUtil.createKey(bucket, keyName,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        value.getBytes(UTF_8));

    // We need to find the location of the chunk file corresponding to the
    // data we just wrote.
    OzoneKey key = bucket.getKey(keyName);
    long containerID = ((OzoneKeyDetails) key).getOzoneKeyLocations().get(0)
        .getContainerID();

    // Get the container by traversing the datanodes. Atleast one of the
    // datanode must have this container.
    Container container = getContainerByID(cluster, containerID);
    assertNotNull(container, "Container not found");
    corruptData(cluster, container, key);
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
      try {
        int len = Math.toIntExact(client.getKeyDetails(volumeName, bucketName, keyName).getDataSize());
        try (InputStream is = client.getKey(volumeName, bucketName, keyName)) {
          Executable read = () -> IOUtils.readFully(is, len);
          if (verifyChecksum) {
            assertThrows(IOException.class, read);
          } else {
            assertDoesNotThrow(read);
          }
        }
      } finally {
        client.close();
      }
    } catch (IOException ignored) {
      // ignore
    }
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
    String keyValue = RandomStringUtils.secure().next(128);
    //String keyValue = "this is a test value.glx";
    // create the initial key with size 0, write will allocate the first block.
    TestDataUtil.createKey(bucket, keyName,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        keyValue.getBytes(UTF_8));

    // First, confirm the key info from the client matches the info in OM.
    OmKeyArgs.Builder builder = new OmKeyArgs.Builder();
    builder.setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName);
    OmKeyLocationInfo keyInfo = ozoneManager.lookupKey(builder.build()).
        getKeyLocationVersions().get(0).getBlocksLatestVersionOnly().get(0);
    long containerID = keyInfo.getContainerID();
    long localID = keyInfo.getLocalID();
    OzoneKeyDetails keyDetails = (OzoneKeyDetails)bucket.getKey(keyName);
    assertEquals(keyName, keyDetails.getName());

    List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();
    assertEquals(1, keyLocations.size());
    assertEquals(containerID, keyLocations.get(0).getContainerID());
    assertEquals(localID, keyLocations.get(0).getLocalID());

    // Make sure that the data size matched.
    assertEquals(keyValue.getBytes(UTF_8).length,
        keyLocations.get(0).getLength());

    // Second, sum the data size from chunks in Container via containerID
    // and localID, make sure the size equals to the size from keyDetails.
    ContainerInfo container = cluster.getStorageContainerManager()
        .getContainerManager().getContainer(ContainerID.valueOf(containerID));
    Pipeline pipeline = cluster.getStorageContainerManager()
        .getPipelineManager().getPipeline(container.getPipelineID());
    List<DatanodeDetails> datanodes = pipeline.getNodes();
    assertEquals(datanodes.size(), 1);

    DatanodeDetails datanodeDetails = datanodes.get(0);
    assertNotNull(datanodeDetails);
    HddsDatanodeService datanodeService = null;
    for (HddsDatanodeService datanodeServiceItr : cluster.getHddsDatanodes()) {
      if (datanodeDetails.equals(datanodeServiceItr.getDatanodeDetails())) {
        datanodeService = datanodeServiceItr;
        break;
      }
    }
    assertNotNull(datanodeService);
    KeyValueContainerData containerData =
        (KeyValueContainerData)(datanodeService.getDatanodeStateMachine()
            .getContainer().getContainerSet().getContainer(containerID)
            .getContainerData());
    try (DBHandle db = BlockUtils.getDB(containerData, cluster.getConf());
         BlockIterator<BlockData> keyValueBlockIterator =
                db.getStore().getBlockIterator(containerID)) {
      while (keyValueBlockIterator.hasNext()) {
        BlockData blockData = keyValueBlockIterator.nextBlock();
        if (blockData.getBlockID().getLocalID() == localID) {
          long length = 0;
          List<ContainerProtos.ChunkInfo> chunks = blockData.getChunks();
          for (ContainerProtos.ChunkInfo chunk : chunks) {
            length += chunk.getLen();
          }
          assertEquals(length, keyValue.getBytes(UTF_8).length);
          break;
        }
      }
    }

    try (OzoneInputStream inputStream = keyDetails.getContent()) {
      assertInputStreamContent(keyValue, inputStream);
    }
  }

  private void assertInputStreamContent(String expected, InputStream is)
      throws IOException {
    byte[] fileContent = new byte[expected.getBytes(UTF_8).length];
    IOUtils.readFully(is, fileContent);
    assertEquals(expected, new String(fileContent, UTF_8));
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
    TestDataUtil.createKey(bucket, keyName,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        value.getBytes(UTF_8));

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
    assertNotNull(container, "Container not found");
    corruptData(cluster, container, key);

    // Try reading the key. Since the chunk file is corrupted, it should
    // throw a checksum mismatch exception.
    IOException ioException = assertThrows(IOException.class, () -> {
      try (OzoneInputStream is = bucket.readKey(keyName)) {
        IOUtils.readFully(is, new byte[100]);
      }
    });

    assertThat(ioException).hasMessageContaining("Checksum mismatch");
  }

  // Make this executed at last, for it has some side effect to other UTs
  @Test
  @Flaky("HDDS-6151")
  void testZReadKeyWithUnhealthyContainerReplica() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName1 = UUID.randomUUID().toString();

    // Write first key
    TestDataUtil.createKey(bucket, keyName1,
        ReplicationConfig.fromTypeAndFactor(RATIS, THREE),
        value.getBytes(UTF_8));

    // Write second key
    String keyName2 = UUID.randomUUID().toString();
    value = "unhealthy container replica";
    TestDataUtil.createKey(bucket, keyName2,
        ReplicationConfig.fromTypeAndFactor(RATIS, THREE),
        value.getBytes(UTF_8));

    // Find container ID
    OzoneKey key = bucket.getKey(keyName2);
    long containerID = ((OzoneKeyDetails) key).getOzoneKeyLocations().get(0)
        .getContainerID();

    // Set container replica to UNHEALTHY
    Container container;
    int index = 1;
    List<HddsDatanodeService> involvedDNs = new ArrayList<>();
    for (HddsDatanodeService hddsDatanode : cluster.getHddsDatanodes()) {
      container = hddsDatanode.getDatanodeStateMachine().getContainer()
          .getContainerSet().getContainer(containerID);
      if (container == null) {
        continue;
      }
      container.markContainerUnhealthy();
      // Change first and second replica commit sequenceId
      if (index < 3) {
        long newBCSID = container.getBlockCommitSequenceId() - 1;
        KeyValueContainerData cData =
            (KeyValueContainerData) container.getContainerData();
        try (DBHandle db = BlockUtils.getDB(cData, cluster.getConf())) {
          db.getStore().getMetadataTable().put(cData.getBcsIdKey(),
              newBCSID);
        }
        container.updateBlockCommitSequenceId(newBCSID);
        index++;
      }
      involvedDNs.add(hddsDatanode);
    }

    // Restart DNs
    int dnCount = involvedDNs.size();
    for (index = 0; index < dnCount; index++) {
      if (index == dnCount - 1) {
        cluster.restartHddsDatanode(
            involvedDNs.get(index).getDatanodeDetails(), true);
      } else {
        cluster.restartHddsDatanode(
            involvedDNs.get(index).getDatanodeDetails(), false);
      }
    }

    StorageContainerManager scm = cluster.getStorageContainerManager();
    GenericTestUtils.waitFor(() -> {
      try {
        ContainerInfo containerInfo = scm.getContainerInfo(containerID);
        System.out.println("state " + containerInfo.getState());
        return containerInfo.getState() == HddsProtos.LifeCycleState.CLOSING;
      } catch (IOException e) {
        fail("Failed to get container info for " + e.getMessage());
        return false;
      }
    }, 1000, 10000);

    // Try reading keyName2
    GenericTestUtils.setLogLevel(XceiverClientGrpc.class, DEBUG);
    try (OzoneInputStream is = bucket.readKey(keyName2)) {
      byte[] content = IOUtils.readFully(is, Math.toIntExact(bucket.getKey(keyName2).getDataSize()));
      String retValue = new String(content, UTF_8);
      assertEquals(value, retValue.trim());
    }
  }

  /**
   * Tests reading a corrputed chunk file throws checksum exception.
   * @throws IOException
   */
  @Test
  void testReadKeyWithCorruptedDataWithMutiNodes() throws IOException {
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
    TestDataUtil.createKey(bucket, keyName,
        ReplicationConfig.fromTypeAndFactor(RATIS, THREE),
        value.getBytes(UTF_8));

    // We need to find the location of the chunk file corresponding to the
    // data we just wrote.
    OzoneKey key = bucket.getKey(keyName);
    List<OzoneKeyLocation> keyLocation =
        ((OzoneKeyDetails) key).getOzoneKeyLocations();
    assertThat(keyLocation).withFailMessage("Key location not found in OM").isNotEmpty();
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
    assertThat(containerList).withFailMessage("Container not found").isNotEmpty();
    corruptData(cluster, containerList.get(0), key);
    // Try reading the key. Read will fail on the first node and will eventually
    // failover to next replica
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] b = new byte[data.length];
      IOUtils.readFully(is, b);
      assertArrayEquals(b, data);
    }
    corruptData(cluster, containerList.get(1), key);
    // Try reading the key. Read will fail on the first node and will eventually
    // failover to next replica
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] b = new byte[data.length];
      IOUtils.readFully(is, b);
      assertArrayEquals(b, data);
    }
    corruptData(cluster, containerList.get(2), key);
    // Try reading the key. Read will fail here as all the replicas are corrupt

    IOException ioException = assertThrows(IOException.class, () -> {
      try (OzoneInputStream is = bucket.readKey(keyName)) {
        byte[] b = new byte[data.length];
        IOUtils.readFully(is, b);
      }
    });
    assertThat(ioException).hasMessageContaining("Checksum mismatch");
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
    TestDataUtil.createKey(bucket, keyName,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        value.getBytes(UTF_8));
    OzoneKey key = bucket.getKey(keyName);
    assertEquals(keyName, key.getName());
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
    BucketLayout bucketLayout = bucket.getBucketLayout();

    if (!bucketLayout.isFileSystemOptimized()) {
      // Rename to an empty string should fail only in non FSO buckets
      OMException oe = assertThrows(OMException.class, () -> bucket.renameKey(fromKeyName, ""));
      assertEquals(ResultCodes.INVALID_KEY_NAME, oe.getResult());
    } else {
      // Rename to an empty key in FSO should be okay, as we are handling the
      // empty dest key on the server side and the source key name will be used
      bucket.renameKey(fromKeyName, "");
      OzoneKey emptyRenameKey = bucket.getKey(fromKeyName);
      assertEquals(fromKeyName, emptyRenameKey.getName());
    }

    String toKeyName = UUID.randomUUID().toString();
    bucket.renameKey(fromKeyName, toKeyName);

    // Lookup for old key should fail.
    assertKeyRenamedEx(bucket, fromKeyName);

    OzoneKey key = bucket.getKey(toKeyName);
    assertEquals(toKeyName, key.getName());
  }

  /**
   * Test of the deprecated rename keys API, which only works on object store
   * or legacy buckets.
   */
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
    volume.createBucket(bucketName, BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.OBJECT_STORE).build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    createTestKey(bucket, keyName1, value);
    createTestKey(bucket, keyName2, value);

    Map<String, String> keyMap = new HashMap();
    keyMap.put(keyName1, newKeyName1);
    keyMap.put(keyName2, newKeyName2);
    bucket.renameKeys(keyMap);

    // new key should exist
    assertEquals(newKeyName1, bucket.getKey(newKeyName1).getName());
    assertEquals(newKeyName2, bucket.getKey(newKeyName2).getName());

    // old key should not exist
    assertKeyRenamedEx(bucket, keyName1);
    assertKeyRenamedEx(bucket, keyName2);
  }

  /**
   * Legacy test for the keys rename API, which is deprecated and only
   * supported for object store and legacy bucket layout types.
   */
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
    volume.createBucket(bucketName, BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.OBJECT_STORE).build());
    OzoneBucket bucket = volume.getBucket(bucketName);

    // Create only keyName1 to test the partial failure of renameKeys.
    createTestKey(bucket, keyName1, value);

    Map<String, String> keyMap = new HashMap();
    keyMap.put(keyName1, newKeyName1);
    keyMap.put(keyName2, newKeyName2);

    try {
      bucket.renameKeys(keyMap);
    } catch (OMException ex) {
      assertEquals(PARTIAL_RENAME, ex.getResult());
    }

    // newKeyName1 should exist
    assertEquals(newKeyName1, bucket.getKey(newKeyName1).getName());
    // newKeyName2 should not exist
    assertKeyRenamedEx(bucket, keyName2);
  }

  @Test
  public void testListVolume() throws IOException {
    String volBase = "vol-list-";
    //Create 10 volume vol-list-a-0-<random> to vol-list-a-9-<random>
    String volBaseNameA = volBase + "a-";
    for (int i = 0; i < 10; i++) {
      store.createVolume(
          volBaseNameA + i + "-" + RandomStringUtils.secure().nextNumeric(5));
    }
    //Create 10 volume vol-list-b-0-<random> to vol-list-b-9-<random>
    String volBaseNameB = volBase + "b-";
    for (int i = 0; i < 10; i++) {
      store.createVolume(
          volBaseNameB + i + "-" + RandomStringUtils.secure().nextNumeric(5));
    }
    Iterator<? extends OzoneVolume> volIterator = store.listVolumes(volBase);
    int totalVolumeCount = 0;
    while (volIterator.hasNext()) {
      volIterator.next();
      totalVolumeCount++;
    }
    assertEquals(20, totalVolumeCount);
    Iterator<? extends OzoneVolume> volAIterator = store.listVolumes(
        volBaseNameA);
    for (int i = 0; i < 10; i++) {
      assertTrue(volAIterator.next().getName()
          .startsWith(volBaseNameA + i + "-"));
    }
    assertFalse(volAIterator.hasNext());
    Iterator<? extends OzoneVolume> volBIterator = store.listVolumes(
        volBaseNameB);
    for (int i = 0; i < 10; i++) {
      assertTrue(volBIterator.next().getName()
          .startsWith(volBaseNameB + i + "-"));
    }
    assertFalse(volBIterator.hasNext());
    Iterator<? extends OzoneVolume> iter = store.listVolumes(volBaseNameA +
        "1-");
    assertTrue(iter.next().getName().startsWith(volBaseNameA + "1-"));
    assertFalse(iter.hasNext());
  }

  @Test
  public void testListBucket()
      throws IOException {
    String volumeA = "vol-a-" + RandomStringUtils.secure().nextNumeric(5);
    String volumeB = "vol-b-" + RandomStringUtils.secure().nextNumeric(5);
    store.createVolume(volumeA);
    store.createVolume(volumeB);
    OzoneVolume volA = store.getVolume(volumeA);
    OzoneVolume volB = store.getVolume(volumeB);


    //Create 10 buckets in  vol-a-<random> and 10 in vol-b-<random>
    String bucketBaseNameA = "bucket-a-";
    for (int i = 0; i < 10; i++) {
      String bucketName = bucketBaseNameA +
          i + "-" + RandomStringUtils.secure().nextNumeric(5);
      volA.createBucket(bucketName);
      store.createSnapshot(volumeA, bucketName, null);
      bucketName = bucketBaseNameA +
          i + "-" + RandomStringUtils.secure().nextNumeric(5);
      volB.createBucket(bucketName);
      store.createSnapshot(volumeB, bucketName, null);
    }
    //Create 10 buckets in vol-a-<random> and 10 in vol-b-<random>
    String bucketBaseNameB = "bucket-b-";
    for (int i = 0; i < 10; i++) {
      String bucketName = bucketBaseNameB +
          i + "-" + RandomStringUtils.secure().nextNumeric(5);
      volA.createBucket(bucketName);
      store.createSnapshot(volumeA, bucketName, null);
      volB.createBucket(
          bucketBaseNameB + i + "-" + RandomStringUtils.secure().nextNumeric(5));
    }
    assertBucketCount(volA, "bucket-", null, false, 20);
    assertBucketCount(volA, "bucket-", null, true, 20);
    assertBucketCount(volB, "bucket-", null, false, 20);
    assertBucketCount(volB, "bucket-", null, true, 10);
    assertBucketCount(volA, bucketBaseNameA, null, false, 10);
    assertBucketCount(volA, bucketBaseNameA, null, true, 10);
    assertBucketCount(volB, bucketBaseNameB, null, false, 10);
    assertBucketCount(volB, bucketBaseNameB, null, true, 0);
    assertBucketCount(volA, bucketBaseNameB, null, false, 10);
    assertBucketCount(volA, bucketBaseNameB, null, true, 10);
    assertBucketCount(volB, bucketBaseNameA, null, false, 10);
    assertBucketCount(volB, bucketBaseNameA, null, true, 10);

  }

  @Test
  public void testListBucketsOnEmptyVolume()
      throws IOException {
    String volume = "vol-empty";
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    Iterator<? extends OzoneBucket> buckets = vol.listBuckets("");
    while (buckets.hasNext()) {
      fail();
    }
  }

  @Test
  public void testListBucketsReplicationConfig()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    getStore().createVolume(volumeName);
    OzoneVolume volume = getStore().getVolume(volumeName);

    // bucket-level replication config: null (default)
    String bucketName = UUID.randomUUID().toString();
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.listBuckets(bucketName).next();
    assertNull(bucket.getReplicationConfig());

    // bucket-level replication config: EC/rs-3-2-1024k
    String ecBucketName = UUID.randomUUID().toString();
    ReplicationConfig ecRepConfig = new ECReplicationConfig(3, 2);
    BucketArgs ecBucketArgs = BucketArgs.newBuilder()
        .setDefaultReplicationConfig(
            new DefaultReplicationConfig(ecRepConfig))
        .build();
    volume.createBucket(ecBucketName, ecBucketArgs);
    OzoneBucket ecBucket = volume.listBuckets(ecBucketName).next();
    assertEquals(ecRepConfig, ecBucket.getReplicationConfig());

    // bucket-level replication config: RATIS/THREE
    String ratisBucketName = UUID.randomUUID().toString();
    ReplicationConfig ratisRepConfig = ReplicationConfig
        .fromTypeAndFactor(RATIS, THREE);
    BucketArgs ratisBucketArgs = BucketArgs.newBuilder()
        .setDefaultReplicationConfig(
            new DefaultReplicationConfig(ratisRepConfig))
        .build();
    volume.createBucket(ratisBucketName, ratisBucketArgs);
    OzoneBucket ratisBucket = volume.listBuckets(ratisBucketName).next();
    assertEquals(ratisRepConfig, ratisBucket.getReplicationConfig());
  }

  @Test
  public void testListKey()
      throws IOException {
    String volumeA = "vol-a-" + RandomStringUtils.secure().nextNumeric(5);
    String volumeB = "vol-b-" + RandomStringUtils.secure().nextNumeric(5);
    String bucketA = "buc-a-" + RandomStringUtils.secure().nextNumeric(5);
    String bucketB = "buc-b-" + RandomStringUtils.secure().nextNumeric(5);
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
      byte[] value = RandomStringUtils.secure().nextAscii(10240).getBytes(UTF_8);
      TestDataUtil.createKey(volAbucketA,
          keyBaseA + i + "-" + RandomStringUtils.secure().nextNumeric(5),
          ReplicationConfig.fromTypeAndFactor(RATIS, ONE), value);
      TestDataUtil.createKey(volAbucketB,
          keyBaseA + i + "-" + RandomStringUtils.secure().nextNumeric(5),
          ReplicationConfig.fromTypeAndFactor(RATIS, ONE), value);
      TestDataUtil.createKey(volBbucketA,
          keyBaseA + i + "-" + RandomStringUtils.secure().nextNumeric(5),
          ReplicationConfig.fromTypeAndFactor(RATIS, ONE), value);
      TestDataUtil.createKey(volBbucketB,
          keyBaseA + i + "-" + RandomStringUtils.secure().nextNumeric(5),
          ReplicationConfig.fromTypeAndFactor(RATIS, ONE), value);
    }
    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseB = "key-b-";
    for (int i = 0; i < 10; i++) {
      byte[] value = RandomStringUtils.secure().nextAscii(10240).getBytes(UTF_8);
      TestDataUtil.createKey(volAbucketA,
          keyBaseB + i + "-" + RandomStringUtils.secure().nextNumeric(5),
          ReplicationConfig.fromTypeAndFactor(RATIS, ONE), value);
      TestDataUtil.createKey(volAbucketB,
          keyBaseB + i + "-" + RandomStringUtils.secure().nextNumeric(5),
          ReplicationConfig.fromTypeAndFactor(RATIS, ONE), value);
      TestDataUtil.createKey(volBbucketA,
          keyBaseB + i + "-" + RandomStringUtils.secure().nextNumeric(5),
          ReplicationConfig.fromTypeAndFactor(RATIS, ONE), value);
      TestDataUtil.createKey(volBbucketB,
          keyBaseB + i + "-" + RandomStringUtils.secure().nextNumeric(5),
          ReplicationConfig.fromTypeAndFactor(RATIS, ONE), value);
    }
    Iterator<? extends OzoneKey> volABucketAIter =
        volAbucketA.listKeys("key-");
    int volABucketAKeyCount = 0;
    while (volABucketAIter.hasNext()) {
      volABucketAIter.next();
      volABucketAKeyCount++;
    }
    assertEquals(20, volABucketAKeyCount);
    Iterator<? extends OzoneKey> volABucketBIter =
        volAbucketB.listKeys("key-");
    int volABucketBKeyCount = 0;
    while (volABucketBIter.hasNext()) {
      volABucketBIter.next();
      volABucketBKeyCount++;
    }
    assertEquals(20, volABucketBKeyCount);
    Iterator<? extends OzoneKey> volBBucketAIter =
        volBbucketA.listKeys("key-");
    int volBBucketAKeyCount = 0;
    while (volBBucketAIter.hasNext()) {
      volBBucketAIter.next();
      volBBucketAKeyCount++;
    }
    assertEquals(20, volBBucketAKeyCount);
    Iterator<? extends OzoneKey> volBBucketBIter =
        volBbucketB.listKeys("key-");
    int volBBucketBKeyCount = 0;
    while (volBBucketBIter.hasNext()) {
      volBBucketBIter.next();
      volBBucketBKeyCount++;
    }
    assertEquals(20, volBBucketBKeyCount);
    Iterator<? extends OzoneKey> volABucketAKeyAIter =
        volAbucketA.listKeys("key-a-");
    int volABucketAKeyACount = 0;
    while (volABucketAKeyAIter.hasNext()) {
      volABucketAKeyAIter.next();
      volABucketAKeyACount++;
    }
    assertEquals(10, volABucketAKeyACount);
    Iterator<? extends OzoneKey> volABucketAKeyBIter =
        volAbucketA.listKeys("key-b-");
    for (int i = 0; i < 10; i++) {
      assertTrue(volABucketAKeyBIter.next().getName()
          .startsWith("key-b-" + i + "-"));
    }
    assertFalse(volABucketBIter.hasNext());
  }

  @Test
  public void testListKeyDirectoriesAreNotFiles()
      throws IOException {
    // Test that directories in multilevel keys are not marked as files

    String volumeA = "volume-a-" + RandomStringUtils.randomNumeric(5);
    String bucketA = "bucket-a-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volumeA);
    OzoneVolume volA = store.getVolume(volumeA);
    volA.createBucket(bucketA);
    OzoneBucket volAbucketA = volA.getBucket(bucketA);

    String keyBaseA = "key-a/";
    for (int i = 0; i < 10; i++) {
      byte[] value = RandomStringUtils.randomAscii(10240).getBytes(UTF_8);
      OzoneOutputStream one = volAbucketA.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, RATIS, ONE,
          new HashMap<>());
      one.write(value);
      one.close();
    }

    Iterator<? extends OzoneKey> volABucketAIter1 = volAbucketA.listKeys(null);
    while (volABucketAIter1.hasNext()) {
      OzoneKey key = volABucketAIter1.next();
      if (key.getName().endsWith("/")) {
        assertFalse(key.isFile(), "Key '" + key.getName() + "' is not a file");
      }
    }

    Iterator<? extends OzoneKey> volABucketAIter2 = volAbucketA.listKeys("key-");
    while (volABucketAIter2.hasNext()) {
      OzoneKey key = volABucketAIter2.next();
      if (key.getName().endsWith("/")) {
        assertFalse(key.isFile(), "Key '" + key.getName() + "' is not a file");
      }
    }
  }

  @Test
  public void testListKeyOnEmptyBucket()
      throws IOException {
    String volume = "vol-" + RandomStringUtils.secure().nextNumeric(5);
    String bucket = "buc-" + RandomStringUtils.secure().nextNumeric(5);
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    vol.createBucket(bucket);
    OzoneBucket buc = vol.getBucket(bucket);
    Iterator<? extends OzoneKey> keys = buc.listKeys("");
    while (keys.hasNext()) {
      fail();
    }
  }

  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testCreateKeyWithMetadataAndTags(BucketLayout bucketLayout) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    OzoneVolume volume = null;
    store.createVolume(volumeName);

    volume = store.getVolume(volumeName);
    BucketArgs bucketArgs =
            BucketArgs.newBuilder().setBucketLayout(bucketLayout).build();
    volume.createBucket(bucketName, bucketArgs);

    OzoneBucket ozoneBucket = volume.getBucket(bucketName);

    Map<String, String> customMetadata = new HashMap<>();
    customMetadata.put("custom-key1", "custom-value1");
    customMetadata.put("custom-key2", "custom-value2");

    Map<String, String> tags = new HashMap<>();
    tags.put("tag-key1", "tag-value1");
    tags.put("tag-key2", "tag-value2");

    writeKey(ozoneBucket, keyName, ONE, value, value.length(), customMetadata, tags);

    OzoneKeyDetails keyDetails = ozoneBucket.getKey(keyName);

    Map<String, String> keyMetadata = keyDetails.getMetadata();

    Map<String, String> keyTags = keyDetails.getTags();

    assertThat(keyMetadata).containsAllEntriesOf(customMetadata);
    assertThat(keyMetadata).doesNotContainKeys("tag-key1", "tag-key2");

    assertThat(keyTags).containsAllEntriesOf(keyTags);
    assertThat(keyTags).doesNotContainKeys("custom-key1", "custom-key2");
  }

  static Stream<ReplicationConfig> replicationConfigs() {
    return Stream.of(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        new ECReplicationConfig(3, 2)
    );
  }

  @ParameterizedTest
  @MethodSource("replicationConfigs")
  void testInitiateMultipartUpload(ReplicationConfig replicationConfig)
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        replicationConfig);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    assertEquals(volumeName, multipartInfo.getVolumeName());
    assertEquals(bucketName, multipartInfo.getBucketName());
    assertEquals(keyName, multipartInfo.getKeyName());
    assertNotNull(multipartInfo.getUploadID());

    // Call initiate multipart upload for the same key again, this should
    // generate a new uploadID.
    multipartInfo = bucket.initiateMultipartUpload(keyName,
        replicationConfig);

    assertNotNull(multipartInfo);
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
    assertNotNull(multipartInfo.getUploadID());

    // Call initiate multipart upload for the same key again, this should
    // generate a new uploadID.
    multipartInfo = bucket.initiateMultipartUpload(keyName);

    assertNotNull(multipartInfo);
    assertNotEquals(multipartInfo.getUploadID(), uploadID);
    assertNotNull(multipartInfo.getUploadID());
  }

  @ParameterizedTest
  @MethodSource("replicationConfigs")
  void testUploadPartWithNoOverride(ReplicationConfig replication)
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String sampleData = "sample Value";

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        replication);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    assertNotNull(multipartInfo.getUploadID());

    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), 1, uploadID);
    ozoneOutputStream.write(string2Bytes(sampleData), 0, sampleData.length());
    ozoneOutputStream.getMetadata().put(ETAG, DigestUtils.md5Hex(sampleData));
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo commitUploadPartInfo = ozoneOutputStream
        .getCommitUploadPartInfo();

    assertNotNull(commitUploadPartInfo);
    assertNotNull(commitUploadPartInfo.getETag());
  }

  @ParameterizedTest
  @MethodSource("replicationConfigs")
  void testUploadPartOverride(ReplicationConfig replication)
      throws IOException {

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
        replication);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    assertNotNull(multipartInfo.getUploadID());

    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), partNumber, uploadID);
    ozoneOutputStream.write(string2Bytes(sampleData), 0, sampleData.length());
    ozoneOutputStream.getMetadata().put(ETAG, DigestUtils.md5Hex(sampleData));
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo commitUploadPartInfo = ozoneOutputStream
        .getCommitUploadPartInfo();

    assertNotNull(commitUploadPartInfo);
    String partName = commitUploadPartInfo.getPartName();
    assertNotNull(commitUploadPartInfo.getETag());

    // Overwrite the part by creating part key with same part number
    // and different content.
    sampleData = "sample Data Changed";
    ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), partNumber, uploadID);
    ozoneOutputStream.write(string2Bytes(sampleData), 0, "name".length());
    ozoneOutputStream.getMetadata().put(ETAG, DigestUtils.md5Hex(sampleData));
    ozoneOutputStream.close();

    commitUploadPartInfo = ozoneOutputStream
        .getCommitUploadPartInfo();

    assertNotNull(commitUploadPartInfo);
    assertNotNull(commitUploadPartInfo.getETag());

    // AWS S3 for same content generates same partName during upload part.
    // In AWS S3 ETag is generated from md5sum. In Ozone right now we
    // don't do this. For now to make things work for large file upload
    // through aws s3 cp, the partName are generated in a predictable fashion.
    // So, when a part is override partNames will still be same irrespective
    // of content in ozone s3. This will make S3 Mpu completeMPU pass when
    // comparing part names and large file uploads work using aws cp.
    assertEquals(partName, commitUploadPartInfo.getPartName(),
        "Part names should be same");
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
    // TODO: HDDS-3402. Files/dirs in FSO buckets currently do not inherit
    //  parent ACLs.
    volume.createBucket(bucketName, BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.OBJECT_STORE).build());
    OzoneBucket bucket = volume.getBucket(bucketName);

    // Add ACL on Bucket
    OzoneAcl acl1 = OzoneAcl.of(USER, "Monday", DEFAULT, ALL);
    OzoneAcl acl2 = OzoneAcl.of(USER, "Friday", DEFAULT, ALL);
    OzoneAcl acl3 = OzoneAcl.of(USER, "Jan", ACCESS, ALL);
    OzoneAcl acl4 = OzoneAcl.of(USER, "Feb", ACCESS, ALL);
    bucket.addAcl(acl1);
    bucket.addAcl(acl2);
    bucket.addAcl(acl3);
    bucket.addAcl(acl4);

    ReplicationConfig replication = RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.ONE);
    doMultipartUpload(bucket, keyName, (byte)98, replication);
    OzoneObj keyObj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName).setKeyName(keyName)
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE).build();
    List<OzoneAcl> aclList = store.getAcl(keyObj);
    // key should inherit bucket's DEFAULT type acl
    assertTrue(aclList.stream().anyMatch(
        acl -> acl.getName().equals(acl1.getName())));
    assertTrue(aclList.stream().anyMatch(
        acl -> acl.getName().equals(acl2.getName())));

    // kye should not inherit bucket's ACCESS type acl
    assertFalse(aclList.stream().anyMatch(
        acl -> acl.getName().equals(acl3.getName())));
    assertFalse(aclList.stream().anyMatch(
        acl -> acl.getName().equals(acl4.getName())));

    // User without permission should fail to upload the object
    String userName = "test-user";
    UserGroupInformation remoteUser =
        UserGroupInformation.createRemoteUser(userName);
    try (OzoneClient client =
        remoteUser.doAs((PrivilegedExceptionAction<OzoneClient>)
            () -> OzoneClientFactory.getRpcClient(cluster.getConf()))) {
      OzoneAcl acl5 = OzoneAcl.of(USER, userName, DEFAULT, ACLType.READ);
      OzoneAcl acl6 = OzoneAcl.of(USER, userName, ACCESS, ACLType.READ);
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
      OMException ome =
          assertThrows(OMException.class, () -> initiateMultipartUpload(bucket2, keyName2, anyReplication()),
              "User without permission should fail");
      assertEquals(ResultCodes.PERMISSION_DENIED, ome.getResult());

      // Add create permission for user, and try multi-upload init again
      OzoneAcl acl7 = OzoneAcl.of(USER, userName, DEFAULT, ACLType.CREATE);
      OzoneAcl acl8 = OzoneAcl.of(USER, userName, ACCESS, ACLType.CREATE);
      OzoneAcl acl9 = OzoneAcl.of(USER, userName, DEFAULT, WRITE);
      OzoneAcl acl10 = OzoneAcl.of(USER, userName, ACCESS, WRITE);
      store.addAcl(volumeObj, acl7);
      store.addAcl(volumeObj, acl8);
      store.addAcl(volumeObj, acl9);
      store.addAcl(volumeObj, acl10);

      store.addAcl(bucketObj, acl7);
      store.addAcl(bucketObj, acl8);
      store.addAcl(bucketObj, acl9);
      store.addAcl(bucketObj, acl10);
      String uploadId = initiateMultipartUpload(bucket2, keyName2,
          anyReplication());

      // Upload part
      byte[] data = generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte) 1);
      Pair<String, String> partNameAndETag = uploadPart(bucket, keyName2,
          uploadId, 1, data);
      Map<Integer, String> eTagsMaps = new TreeMap<>();
      eTagsMaps.put(1, partNameAndETag.getValue());

      // Complete multipart upload request
      completeMultipartUpload(bucket2, keyName2, uploadId, eTagsMaps);

      // User without permission cannot read multi-uploaded object
      OMException ex = assertThrows(OMException.class, () -> {
        try (OzoneInputStream ignored = bucket2.readKey(keyName)) {
          LOG.error("User without permission should fail");
        }
      }, "User without permission should fail");
      assertEquals(ResultCodes.PERMISSION_DENIED, ex.getResult());
    }
  }

  @ParameterizedTest
  @MethodSource("replicationConfigs")
  void testMultipartUploadOverride(ReplicationConfig replication)
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    doMultipartUpload(bucket, keyName, (byte)96, replication);

    // Initiate Multipart upload again, now we should read latest version, as
    // read always reads latest blocks.
    doMultipartUpload(bucket, keyName, (byte)97, replication);

  }

  /**
   * This test prints out that there is a memory leak in the test logs
   * which during post-processing is caught by the CI thereby failing the
   * CI run. Hence, disabling this for CI.
   */
  @Unhealthy
  public void testClientLeakDetector() throws Exception {
    OzoneClient client = OzoneClientFactory.getRpcClient(cluster.getConf());
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    LogCapturer ozoneClientFactoryLogCapturer =
        LogCapturer.captureLogs(
            OzoneClientFactory.class);

    client.getObjectStore().createVolume(volumeName);
    OzoneVolume volume = client.getObjectStore().getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    byte[] data = new byte[10];
    Arrays.fill(data, (byte) 1);
    TestDataUtil.createKey(bucket, keyName,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        data);
    client = null;
    System.gc();
    GenericTestUtils.waitFor(() -> ozoneClientFactoryLogCapturer.getOutput()
        .contains("is not closed properly"), 100, 2000);
  }

  @Test
  public void testMultipartUploadOwner() throws Exception {
    // Save the old user, and switch to the old user after test
    UserGroupInformation oldUser = UserGroupInformation.getCurrentUser();
    try {
      String volumeName = UUID.randomUUID().toString();
      String bucketName = UUID.randomUUID().toString();
      String keyName1 = UUID.randomUUID().toString();
      String keyName2 = UUID.randomUUID().toString();
      UserGroupInformation user1 = UserGroupInformation
          .createUserForTesting("user1", new String[]{"user1"});
      UserGroupInformation awsUser1 = UserGroupInformation
          .createUserForTesting("awsUser1", new String[]{"awsUser1"});
      ReplicationConfig replication = RatisReplicationConfig.getInstance(
          HddsProtos.ReplicationFactor.THREE);

      // create volume and bucket and add ACL
      store.createVolume(volumeName);
      store.getVolume(volumeName).createBucket(bucketName);
      OzoneObj volumeObj = OzoneObjInfo.Builder.newBuilder()
          .setVolumeName(volumeName).setStoreType(OzoneObj.StoreType.OZONE)
          .setResType(OzoneObj.ResourceType.VOLUME).build();
      OzoneObj bucketObj = OzoneObjInfo.Builder.newBuilder()
          .setVolumeName(volumeName).setBucketName(bucketName)
          .setStoreType(OzoneObj.StoreType.OZONE)
          .setResType(OzoneObj.ResourceType.BUCKET).build();
      store.addAcl(volumeObj, OzoneAcl.of(USER, "user1", ACCESS, ALL));
      store.addAcl(volumeObj, OzoneAcl.of(USER, "awsUser1", ACCESS, ALL));
      store.addAcl(bucketObj, OzoneAcl.of(USER, "user1", ACCESS, ALL));
      store.addAcl(bucketObj, OzoneAcl.of(USER, "awsUser1", ACCESS, ALL));

      // user1 MultipartUpload a key
      UserGroupInformation.setLoginUser(user1);
      setOzClient(OzoneClientFactory.getRpcClient(cluster.getConf()));
      setStore(ozClient.getObjectStore());
      OzoneBucket bucket = store.getVolume(volumeName).getBucket(bucketName);
      doMultipartUpload(bucket, keyName1, (byte) 96, replication);

      assertEquals(user1.getShortUserName(),
          bucket.getKey(keyName1).getOwner());

      // After HDDS-5881 the user will not be different,
      // as S3G uses single RpcClient.
      // * performing the operation. the real user is an AWS user
      // form AWS client.
      String strToSign = "AWS4-HMAC-SHA256\n" +
          "20150830T123600Z\n" +
          "20150830/us-east-1/iam/aws4_request\n" +
          "f536975d06c0309214f805bb90ccff089219ecd68b2" +
          "577efef23edd43b7e1a59";
      String signature =  "5d672d79c15b13162d9279b0855cfba" +
          "6789a8edb4c82c400e06b5924a6f2b5d7";
      String secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
      S3Auth s3Auth = new S3Auth(strToSign, signature,
          awsUser1.getShortUserName(), awsUser1.getShortUserName());
      // Add secret to S3Secret table.
      S3SecretManager s3SecretManager = cluster.getOzoneManager()
          .getS3SecretManager();
      s3SecretManager.storeSecret(awsUser1.getShortUserName(),
          S3SecretValue.of(awsUser1.getShortUserName(), secret));
      setOzClient(OzoneClientFactory.getRpcClient(cluster.getConf()));
      setStore(ozClient.getObjectStore());

      // set AWS user for RPCClient and OzoneManager
      store.getClientProxy().setThreadLocalS3Auth(s3Auth);
      OzoneManager.setS3Auth(OzoneManagerProtocolProtos.S3Authentication
          .newBuilder().setAccessId(awsUser1.getUserName()).build());
      // awsUser1 create a key
      bucket = store.getVolume(volumeName).getBucket(bucketName);
      doMultipartUpload(bucket, keyName2, (byte)96, replication);

      assertEquals(awsUser1.getShortUserName(),
          bucket.getKey(keyName2).getOwner());
    } finally {
      OzoneManager.setS3Auth(null);
      UserGroupInformation.setLoginUser(oldUser);
      setOzClient(OzoneClientFactory.getRpcClient(cluster.getConf()));
      setStore(ozClient.getObjectStore());
    }
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
    String uploadID = initiateMultipartUpload(bucket, keyName,
        anyReplication());

    // Upload Parts
    Map<Integer, String> eTagsMaps = new TreeMap<>();
    // Uploading part 1 with less than min size
    Pair<String, String> partNameAndETag = uploadPart(bucket, keyName,
        uploadID, 1, "data".getBytes(UTF_8));
    eTagsMaps.put(1, partNameAndETag.getValue());

    partNameAndETag = uploadPart(bucket, keyName, uploadID, 2,
        "data".getBytes(UTF_8));
    eTagsMaps.put(2, partNameAndETag.getValue());


    // Complete multipart upload

    OzoneTestUtils.expectOmException(ResultCodes.ENTITY_TOO_SMALL,
        () -> completeMultipartUpload(bucket, keyName, uploadID, eTagsMaps));

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

    String uploadID = initiateMultipartUpload(bucket, keyName,
        anyReplication());

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

    ReplicationConfig replication = RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.ONE);
    String uploadID = initiateMultipartUpload(bucket, keyName, replication);

    uploadPart(bucket, keyName, uploadID, 1, "data".getBytes(UTF_8));
    // We have not uploaded any parts, but passing some list it should throw
    // error.
    TreeMap<Integer, String> eTagsMaps = new TreeMap<>();
    eTagsMaps.put(1, DigestUtils.md5Hex(UUID.randomUUID().toString()));

    OzoneTestUtils.expectOmException(ResultCodes.INVALID_PART,
        () -> completeMultipartUpload(bucket, keyName, uploadID, eTagsMaps));

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

    ReplicationConfig replication = RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.ONE);
    String uploadID = initiateMultipartUpload(bucket, keyName, replication);

    uploadPart(bucket, keyName, uploadID, 1, "data".getBytes(UTF_8));
    // We have not uploaded any parts, but passing some list it should throw
    // error.
    TreeMap<Integer, String> eTagsMap = new TreeMap<>();
    eTagsMap.put(3, DigestUtils.md5Hex("random"));

    OzoneTestUtils.expectOmException(ResultCodes.INVALID_PART,
        () -> completeMultipartUpload(bucket, keyName, uploadID, eTagsMap));
  }

  @Test
  public void testMultipartPartNumberExceedingAllowedRange() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String sampleData = "sample Value";

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName);
    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();

    // Multipart part number must be an integer between 1 and 10000. So the
    // part number 1, 5000, 10000 will succeed,
    // the part number 0, 10001 will fail.
    bucket.createMultipartKey(keyName, sampleData.length(), 1, uploadID);
    bucket.createMultipartKey(keyName, sampleData.length(), 5000, uploadID);
    bucket.createMultipartKey(keyName, sampleData.length(), 10000, uploadID);
    OzoneTestUtils.expectOmException(ResultCodes.INVALID_PART, () ->
        bucket.createMultipartKey(
            keyName, sampleData.length(), 0, uploadID));
    OzoneTestUtils.expectOmException(ResultCodes.INVALID_PART, () ->
        bucket.createMultipartKey(
            keyName, sampleData.length(), 10001, uploadID));
  }

  @ParameterizedTest
  @MethodSource("replicationConfigs")
  public void testMultipartUploadWithCustomMetadata(ReplicationConfig replication) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    // Create custom metadata
    Map<String, String> customMetadata = new HashMap<>();
    customMetadata.put("custom-key1", "custom-value1");
    customMetadata.put("custom-key2", "custom-value2");

    doMultipartUpload(bucket, keyName, (byte) 98, replication, customMetadata, Collections.emptyMap());
  }

  @ParameterizedTest
  @MethodSource({"replicationConfigs"})
  public void testMultipartUploadWithTags(ReplicationConfig replication) throws Exception {
    testMultipartUploadWithTags(replication, BucketLayout.OBJECT_STORE);
  }

  @ParameterizedTest
  @MethodSource({"bucketLayouts"})
  public void testMultipartUploadWithTags(BucketLayout bucketLayout) throws Exception {
    testMultipartUploadWithTags(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE), bucketLayout);
  }

  private void testMultipartUploadWithTags(ReplicationConfig replication, BucketLayout bucketLayout)
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);

    BucketArgs bucketArgs =
        BucketArgs.newBuilder().setBucketLayout(bucketLayout).build();
    volume.createBucket(bucketName, bucketArgs);
    OzoneBucket bucket = volume.getBucket(bucketName);

    // Create tags
    Map<String, String> tags = new HashMap<>();
    tags.put("tag-key1", "tag-value1");
    tags.put("tag-key2", "tag-value2");

    doMultipartUpload(bucket, keyName, (byte) 96, replication, Collections.emptyMap(), tags);
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
  void testAbortUploadFailWithInProgressPartUpload() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    OmMultipartInfo omMultipartInfo = bucket.initiateMultipartUpload(keyName,
        anyReplication());

    assertNotNull(omMultipartInfo.getUploadID());

    // Do not close output stream.
    byte[] data = "data".getBytes(UTF_8);
    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, 1, omMultipartInfo.getUploadID());
    ozoneOutputStream.write(data, 0, data.length);

    // Abort before completing part upload.
    bucket.abortMultipartUpload(keyName, omMultipartInfo.getUploadID());
    OMException ome = assertThrows(OMException.class, () -> ozoneOutputStream.close());
    assertEquals(NO_SUCH_MULTIPART_UPLOAD_ERROR, ome.getResult());
  }

  @Test
  void testCommitPartAfterCompleteUpload() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    OmMultipartInfo omMultipartInfo = bucket.initiateMultipartUpload(keyName,
        anyReplication());

    assertNotNull(omMultipartInfo.getUploadID());

    String uploadID = omMultipartInfo.getUploadID();

    // upload part 1.
    byte[] data = generateData(5 * 1024 * 1024,
        (byte) RandomUtils.secure().randomLong());
    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, 1, uploadID);
    ozoneOutputStream.write(data, 0, data.length);
    ozoneOutputStream.getMetadata().put(ETAG,
        DatatypeConverter.printHexBinary(eTagProvider.digest(data))
            .toLowerCase());
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
        ozoneOutputStream.getCommitUploadPartInfo();

    // Do not close output stream for part 2.
    ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, 2, omMultipartInfo.getUploadID());
    ozoneOutputStream.getMetadata().put(ETAG,
        DatatypeConverter.printHexBinary(eTagProvider.digest(data))
            .toLowerCase());
    ozoneOutputStream.write(data, 0, data.length);

    Map<Integer, String> partsMap = new LinkedHashMap<>();
    partsMap.put(1, omMultipartCommitUploadPartInfo.getETag());
    completeMultipartUpload(bucket, keyName, uploadID, partsMap);

    byte[] fileContent = new byte[data.length];
    try (OzoneInputStream inputStream = bucket.readKey(keyName)) {
      IOUtils.readFully(inputStream, fileContent);
    }
    StringBuilder sb = new StringBuilder(data.length);

    // Combine all parts data, and check is it matching with get key data.
    String part1 = new String(data, UTF_8);
    sb.append(part1);
    assertEquals(sb.toString(), new String(fileContent, UTF_8));
    OMException ex = assertThrows(OMException.class, ozoneOutputStream::close);
    assertEquals(NO_SUCH_MULTIPART_UPLOAD_ERROR, ex.getResult());
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

    String uploadID = initiateMultipartUpload(bucket, keyName,
        anyReplication());
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

    String uploadID = initiateMultipartUpload(bucket, keyName,
        anyReplication());
    uploadPart(bucket, keyName, uploadID, 1, "data".getBytes(UTF_8));
    bucket.abortMultipartUpload(keyName, uploadID);
  }

  @ParameterizedTest
  @MethodSource("replicationConfigs")
  void testListMultipartUploadParts(ReplicationConfig replication)
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    Map<Integer, String> partsMap = new TreeMap<>();
    String uploadID = initiateMultipartUpload(bucket, keyName, replication);
    Pair<String, String> partNameAndETag1 = uploadPart(bucket, keyName,
        uploadID, 1, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(1, partNameAndETag1.getKey());

    Pair<String, String> partNameAndETag2 = uploadPart(bucket, keyName,
        uploadID, 2, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(2, partNameAndETag2.getKey());

    Pair<String, String> partNameAndETag3 = uploadPart(bucket, keyName,
        uploadID, 3, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(3, partNameAndETag3.getKey());

    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 0, 3);

    assertEquals(
        replication,
        ozoneMultipartUploadPartListParts.getReplicationConfig());

    assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(0).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(0)
            .getPartName());
    assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(1).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(1)
            .getPartName());
    assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(2).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(2)
            .getPartName());

    assertFalse(ozoneMultipartUploadPartListParts.isTruncated());
  }

  @ParameterizedTest
  @MethodSource("replicationConfigs")
  void testListMultipartUploadPartsWithContinuation(
      ReplicationConfig replication) throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    Map<Integer, String> partsMap = new TreeMap<>();
    String uploadID = initiateMultipartUpload(bucket, keyName, replication);
    Pair<String, String> partNameAndETag1 = uploadPart(bucket, keyName,
        uploadID, 1, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(1, partNameAndETag1.getKey());

    Pair<String, String> partNameAndETag2 = uploadPart(bucket, keyName,
        uploadID, 2, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(2, partNameAndETag2.getKey());

    Pair<String, String> partNameAndETag3 = uploadPart(bucket, keyName,
        uploadID, 3, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(3, partNameAndETag3.getKey());

    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 0, 2);

    assertEquals(replication,
        ozoneMultipartUploadPartListParts.getReplicationConfig());

    assertEquals(2,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());

    assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(0).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(0)
            .getPartName());
    assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(1).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(1)
            .getPartName());

    // Get remaining
    assertTrue(ozoneMultipartUploadPartListParts.isTruncated());
    ozoneMultipartUploadPartListParts = bucket.listParts(keyName, uploadID,
        ozoneMultipartUploadPartListParts.getNextPartNumberMarker(), 2);

    assertEquals(1,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());
    assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(0).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(0)
            .getPartName());


    // As we don't have any parts for this, we should get false here
    assertFalse(ozoneMultipartUploadPartListParts.isTruncated());

  }

  @ParameterizedTest
  @CsvSource(value = {"-1,1,Should be greater than or equal to zero", "2,-1,Max Parts Should be greater than zero"})
  public void testListPartsInvalidInput(int partNumberMarker, int maxParts, String exceptedMessage) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> bucket.listParts(keyName, "random", partNumberMarker, maxParts));

    assertThat(exception).hasMessageContaining(exceptedMessage);
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


    String uploadID = initiateMultipartUpload(bucket, keyName,
        anyReplication());
    uploadPart(bucket, keyName, uploadID, 1,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));


    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 100, 2);

    // Should return empty

    assertEquals(0,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());

    // As we don't have any parts with greater than partNumberMarker and list
    // is not truncated, so it should return false here.
    assertFalse(ozoneMultipartUploadPartListParts.isTruncated());

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
    assertNotNull(bucket, "Bucket creation failed");

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
    assertThat(acls).contains(defaultUserAcl);
    assertThat(acls).contains(defaultGroupAcl);

    acls = store.getAcl(childObj);
    assertThat(acls).contains(inheritedUserAcl);
    assertThat(acls).contains(inheritedGroupAcl);
  }

  @Test
  public void testNativeAclsForKey() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String key1 = "dir1/dir2" + UUID.randomUUID();
    String key2 = "dir1/dir2" + UUID.randomUUID();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertNotNull(bucket, "Bucket creation failed");

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
    assertThat(acls).contains(inheritedUserAcl);
    assertThat(acls).contains(inheritedGroupAcl);
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
    assertNotNull(bucket, "Bucket creation failed");

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

    OzoneAcl user1Acl = OzoneAcl.of(USER, "user1", ACCESS, READ);
    assertTrue(store.addAcl(prefixObj, user1Acl));

    // get acl
    List<OzoneAcl> aclsGet = store.getAcl(prefixObj);
    assertEquals(1, aclsGet.size());
    assertEquals(user1Acl, aclsGet.get(0));

    // remove acl
    assertTrue(store.removeAcl(prefixObj, user1Acl));
    aclsGet = store.getAcl(prefixObj);
    assertEquals(0, aclsGet.size());

    OzoneAcl group1Acl = OzoneAcl.of(GROUP, "group1", ACCESS, ALL);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(user1Acl);
    acls.add(group1Acl);
    assertTrue(store.setAcl(prefixObj, acls));

    // get acl
    aclsGet = store.getAcl(prefixObj);
    assertEquals(2, aclsGet.size());

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
    OmConfig omConfig = conf.getObject(OmConfig.class);

    listOfAcls.add(OzoneAcl.of(USER, ugi.getShortUserName(), ACCESS, omConfig.getUserDefaultRights()));
    //Group ACL of the User
    listOfAcls.add(OzoneAcl.of(GROUP, ugi.getPrimaryGroupName(), ACCESS, omConfig.getGroupDefaultRights()));
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
    if (!expectedAcls.isEmpty()) {
      OzoneAcl oldAcl = expectedAcls.get(0);
      OzoneAcl newAcl = OzoneAcl.of(oldAcl.getType(), oldAcl.getName(),
          oldAcl.getAclScope(), ACLType.READ_ACL);
      // Verify that operation successful.
      assertTrue(store.addAcl(ozObj, newAcl));

      assertEquals(expectedAcls.size(), store.getAcl(ozObj).size());
      final Optional<OzoneAcl> readAcl = store.getAcl(ozObj).stream()
          .filter(acl -> acl.getName().equals(newAcl.getName())
              && acl.getType().equals(newAcl.getType()))
          .findFirst();
      assertTrue(readAcl.isPresent(), "New acl expected but not found.");
      assertThat(readAcl.get().getAclList()).contains(ACLType.READ_ACL);


      // Case:2 Remove newly added acl permission.
      assertTrue(store.removeAcl(ozObj, newAcl));

      assertEquals(expectedAcls.size(), store.getAcl(ozObj).size());
      final Optional<OzoneAcl> nonReadAcl = store.getAcl(ozObj).stream()
          .filter(acl -> acl.getName().equals(newAcl.getName())
              && acl.getType().equals(newAcl.getType()))
          .findFirst();
      assertTrue(nonReadAcl.isPresent(), "New acl expected but not found.");
      assertThat(nonReadAcl.get().getAclList()).doesNotContain(ACLType.READ_ACL);
    } else {
      fail("Default acl should not be empty.");
    }

    List<OzoneAcl> keyAcls = store.getAcl(ozObj);
    assertThat(keyAcls).containsAll(expectedAcls);

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
    assertThat(finalNewAcls).containsAll(expectedAcls);

    // Reset acl's.
    OzoneAcl ua = OzoneAcl.of(USER, "userx",
        ACCESS, ACLType.READ_ACL);
    OzoneAcl ug = OzoneAcl.of(GROUP, "userx",
        ACCESS, ALL);
    store.setAcl(ozObj, Arrays.asList(ua, ug));
    newAcls = store.getAcl(ozObj);
    assertEquals(2, newAcls.size());
    assertThat(newAcls).contains(ua);
    assertThat(newAcls).contains(ug);
  }

  private void writeKey(String key1, OzoneBucket bucket) throws IOException {
    TestDataUtil.createKey(bucket, key1,
        ReplicationConfig.fromTypeAndFactor(RATIS, ONE),
        RandomStringUtils.secure().next(1024).getBytes(UTF_8));
  }

  private byte[] generateData(int size, byte val) {
    byte[] chars = new byte[size];
    Arrays.fill(chars, val);
    return chars;
  }

  private void doMultipartUpload(OzoneBucket bucket, String keyName, byte val,
      ReplicationConfig replication)
      throws Exception {
    doMultipartUpload(bucket, keyName, val, replication, Collections.emptyMap(), Collections.emptyMap());
  }

  private void doMultipartUpload(OzoneBucket bucket, String keyName, byte val,
      ReplicationConfig replication, Map<String, String> customMetadata, Map<String, String> tags)
      throws Exception {
    // Initiate Multipart upload request
    String uploadID = initiateMultipartUpload(bucket, keyName, replication, customMetadata, tags);

    // Upload parts
    Map<Integer, String> partsMap = new TreeMap<>();

    // get 5mb data, as each part should be of min 5mb, last part can be less
    // than 5mb
    int length = 0;
    byte[] data = generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, val);
    Pair<String, String> partNameAndEtag = uploadPart(bucket, keyName, uploadID,
        1, data);
    partsMap.put(1, partNameAndEtag.getValue());
    length += data.length;


    partNameAndEtag = uploadPart(bucket, keyName, uploadID, 2, data);
    partsMap.put(2, partNameAndEtag.getValue());
    length += data.length;

    String part3 = UUID.randomUUID().toString();
    partNameAndEtag = uploadPart(bucket, keyName, uploadID, 3, part3.getBytes(
        UTF_8));
    partsMap.put(3, partNameAndEtag.getValue());
    length += part3.getBytes(UTF_8).length;

    // Complete multipart upload request
    completeMultipartUpload(bucket, keyName, uploadID, partsMap);

    //Now Read the key which has been completed multipart upload.
    byte[] fileContent = new byte[data.length + data.length + part3.getBytes(
        UTF_8).length];
    try (OzoneInputStream inputStream = bucket.readKey(keyName)) {
      IOUtils.readFully(inputStream, fileContent);
    }

    verifyReplication(bucket.getVolumeName(), bucket.getName(), keyName,
        replication);

    StringBuilder sb = new StringBuilder(length);

    // Combine all parts data, and check is it matching with get key data.
    String part1 = new String(data, UTF_8);
    String part2 = new String(data, UTF_8);
    sb.append(part1);
    sb.append(part2);
    sb.append(part3);
    assertEquals(sb.toString(), new String(fileContent, UTF_8));

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setKeyName(keyName)
        .build();
    ResolvedBucket resolvedBucket = new ResolvedBucket(
        bucket.getVolumeName(), bucket.getName(),
        bucket.getVolumeName(), bucket.getName(),
        "", bucket.getBucketLayout());
    OmKeyInfo omKeyInfo = ozoneManager.getKeyManager().getKeyInfo(keyArgs,
        resolvedBucket, UUID.randomUUID().toString());

    OmKeyLocationInfoGroup latestVersionLocations =
        omKeyInfo.getLatestVersionLocations();
    assertNotNull(latestVersionLocations);
    assertTrue(latestVersionLocations.isMultipartKey());
    latestVersionLocations.getBlocksLatestVersionOnly()
        .forEach(omKeyLocationInfo ->
            assertNotEquals(-1, omKeyLocationInfo.getPartNumber()));

    Map<String, String> keyMetadata = omKeyInfo.getMetadata();
    assertNotNull(keyMetadata.get(ETAG));
    if (customMetadata != null && !customMetadata.isEmpty()) {
      assertThat(keyMetadata).containsAllEntriesOf(customMetadata);
    }

    Map<String, String> keyTags = omKeyInfo.getTags();
    if (keyTags != null && !keyTags.isEmpty()) {
      assertThat(keyTags).containsAllEntriesOf(tags);
    }
  }

  private String initiateMultipartUpload(OzoneBucket bucket, String keyName,
      ReplicationConfig replicationConfig) throws Exception {
    return initiateMultipartUpload(bucket, keyName, replicationConfig, Collections.emptyMap(), Collections.emptyMap());
  }

  private String initiateMultipartUpload(OzoneBucket bucket, String keyName,
      ReplicationConfig replicationConfig, Map<String, String> customMetadata,
      Map<String, String> tags) throws Exception {
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        replicationConfig, customMetadata, tags);

    String uploadID = multipartInfo.getUploadID();
    assertNotNull(uploadID);
    return uploadID;
  }

  private Pair<String, String> uploadPart(OzoneBucket bucket, String keyName,
                                          String uploadID, int partNumber,
                                          byte[] data) throws Exception {
    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, partNumber, uploadID);
    ozoneOutputStream.write(data, 0,
        data.length);
    ozoneOutputStream.getMetadata().put(ETAG,
        DatatypeConverter.printHexBinary(eTagProvider.digest(data))
            .toLowerCase());
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
        ozoneOutputStream.getCommitUploadPartInfo();

    assertNotNull(omMultipartCommitUploadPartInfo);
    assertNotNull(omMultipartCommitUploadPartInfo.getETag());
    assertNotNull(omMultipartCommitUploadPartInfo.getPartName());
    return Pair.of(omMultipartCommitUploadPartInfo.getPartName(),
        omMultipartCommitUploadPartInfo.getETag());

  }

  private void completeMultipartUpload(OzoneBucket bucket, String keyName,
      String uploadID, Map<Integer, String> partsMap) throws Exception {
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo = bucket
        .completeMultipartUpload(keyName, uploadID, partsMap);

    assertNotNull(omMultipartUploadCompleteInfo);
    assertEquals(omMultipartUploadCompleteInfo.getKey(), keyName);
    assertNotNull(omMultipartUploadCompleteInfo.getHash());
  }

  private OzoneKeyDetails createTestKey(OzoneBucket bucket) throws IOException {
    return createTestKey(bucket, getTestName(), UUID.randomUUID().toString());
  }

  private OzoneKeyDetails createTestKey(
      OzoneBucket bucket, String keyName, String keyValue
  ) throws IOException {
    return createTestKey(bucket, keyName, keyValue.getBytes(UTF_8));
  }

  private OzoneKeyDetails createTestKey(
      OzoneBucket bucket, String keyName, byte[] bytes
  ) throws IOException {
    RatisReplicationConfig replication = RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE);
    Map<String, String> metadata = singletonMap("key", RandomStringUtils.secure().nextAscii(10));
    try (OzoneOutputStream out = bucket.createKey(keyName, bytes.length, replication, metadata)) {
      out.write(bytes);
    }
    OzoneKeyDetails key = bucket.getKey(keyName);
    assertNotNull(key);
    assertEquals(keyName, key.getName());
    return key;
  }

  private void assertKeyRenamedEx(OzoneBucket bucket, String keyName) {
    OMException oe = assertThrows(OMException.class, () -> bucket.getKey(keyName));
    assertEquals(KEY_NOT_FOUND, oe.getResult());
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
    // This test uses object store layout to make manual key modifications
    // easier.
    BucketArgs args = BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.OBJECT_STORE)
        .addMetadata(OzoneConsts.GDPR_FLAG, "true").build();
    volume.createBucket(bucketName, args);
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertEquals(bucketName, bucket.getName());
    assertNotNull(bucket.getMetadata());
    assertEquals("true",
        bucket.getMetadata().get(OzoneConsts.GDPR_FLAG));

    //Step 2
    String text = "hello world";
    Map<String, String> keyMetadata = new HashMap<>();
    keyMetadata.put(OzoneConsts.GDPR_FLAG, "true");
    OzoneOutputStream out = bucket.createKey(keyName,
        text.getBytes(UTF_8).length, RATIS, ONE, keyMetadata);
    out.write(text.getBytes(UTF_8));
    out.close();
    assertNull(keyMetadata.get(OzoneConsts.GDPR_SECRET));

    //Step 3
    OzoneKeyDetails key = bucket.getKey(keyName);

    assertEquals(keyName, key.getName());
    assertEquals("true", key.getMetadata().get(OzoneConsts.GDPR_FLAG));
    assertEquals("AES",
        key.getMetadata().get(OzoneConsts.GDPR_ALGORITHM));
    assertNotNull(key.getMetadata().get(OzoneConsts.GDPR_SECRET));

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      assertInputStreamContent(text, is);
      verifyReplication(volumeName, bucketName, keyName,
          RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE));
    }
    //Step 4
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(BucketLayout.OBJECT_STORE)
        .get(omMetadataManager.getOzoneKey(volumeName, bucketName, keyName));

    omKeyInfo = omKeyInfo.withMetadataMutations(
        metadata -> metadata.remove(OzoneConsts.GDPR_FLAG));

    omMetadataManager.getKeyTable(BucketLayout.OBJECT_STORE)
        .put(omMetadataManager.getOzoneKey(volumeName, bucketName, keyName),
            omKeyInfo);

    //Step 5
    key = bucket.getKey(keyName);
    assertEquals(keyName, key.getName());
    assertNull(key.getMetadata().get(OzoneConsts.GDPR_FLAG));

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] fileContent = new byte[text.getBytes(UTF_8).length];
      IOUtils.readFully(is, fileContent);

      //Step 6
      assertNotEquals(text, new String(fileContent, UTF_8));
    }
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
    assertEquals(bucketName, bucket.getName());
    assertNotNull(bucket.getMetadata());
    assertEquals("true",
        bucket.getMetadata().get(OzoneConsts.GDPR_FLAG));

    //Step 2
    String text = "hello world";
    Map<String, String> keyMetadata = new HashMap<>();
    keyMetadata.put(OzoneConsts.GDPR_FLAG, "true");
    OzoneOutputStream out = bucket.createKey(keyName,
        text.getBytes(UTF_8).length, RATIS, ONE, keyMetadata);
    out.write(text.getBytes(UTF_8));
    out.close();

    //Step 3
    OzoneKeyDetails key = bucket.getKey(keyName);

    assertEquals(keyName, key.getName());
    assertEquals("true", key.getMetadata().get(OzoneConsts.GDPR_FLAG));
    assertEquals("AES",
        key.getMetadata().get(OzoneConsts.GDPR_ALGORITHM));
    assertNotNull(key.getMetadata().get(OzoneConsts.GDPR_SECRET));

    try (OzoneInputStream is = bucket.readKey(keyName)) {
      assertInputStreamContent(text, is);
    }
    verifyReplication(volumeName, bucketName, keyName,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE));

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
      assertThat(deletedKeyMetadata).doesNotContainKey(OzoneConsts.GDPR_FLAG);
      assertThat(deletedKeyMetadata).doesNotContainKey(OzoneConsts.GDPR_SECRET);
      assertThat(deletedKeyMetadata).doesNotContainKey(OzoneConsts.GDPR_ALGORITHM);
    }
  }

  @Test
  public void testSetS3VolumeAcl() throws Exception {
    OzoneObj s3vVolume = new OzoneObjInfo.Builder()
        .setVolumeName(
            HddsClientUtils.getDefaultS3VolumeName(cluster.getConf()))
        .setResType(OzoneObj.ResourceType.VOLUME)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    OzoneAcl ozoneAcl = OzoneAcl.of(USER, remoteUserName, DEFAULT, WRITE);

    boolean result = store.addAcl(s3vVolume, ozoneAcl);

    assertTrue(result, "SetAcl on default s3v failed");

    List<OzoneAcl> ozoneAclList = store.getAcl(s3vVolume);

    assertThat(ozoneAclList).contains(ozoneAcl);
  }

  @Test
  public void testHeadObject() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    ReplicationConfig replicationConfig = ReplicationConfig
        .fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE);

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);


    String keyName = UUID.randomUUID().toString();

    TestDataUtil.createKey(bucket, keyName,
        replicationConfig, value.getBytes(UTF_8));

    OzoneKey key = bucket.headObject(keyName);
    assertEquals(volumeName, key.getVolumeName());
    assertEquals(bucketName, key.getBucketName());
    assertEquals(keyName, key.getName());
    assertEquals(replicationConfig.getReplicationType(),
        key.getReplicationConfig().getReplicationType());
    assertEquals(replicationConfig.getRequiredNodes(),
        key.getReplicationConfig().getRequiredNodes());
    assertEquals(value.getBytes(UTF_8).length, key.getDataSize());

    try {
      bucket.headObject(UUID.randomUUID().toString());
    } catch (OMException ex) {
      assertEquals(ResultCodes.KEY_NOT_FOUND, ex.getResult());
    }

  }

  private void createRequiredForVersioningTest(String volumeName,
      String bucketName, String keyName, boolean versioning) throws Exception {

    ReplicationConfig replicationConfig = ReplicationConfig
        .fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE);

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);

    // This test inspects RocksDB delete table to check for versioning
    // information. This is easier to do with object store keys.
    volume.createBucket(bucketName, BucketArgs.newBuilder()
        .setVersioning(versioning)
        .setBucketLayout(VERSIONING_TEST_BUCKET_LAYOUT).build());
    OzoneBucket bucket = volume.getBucket(bucketName);

    TestDataUtil.createKey(bucket, keyName,
        replicationConfig, value.getBytes(UTF_8));

    // Override key
    TestDataUtil.createKey(bucket, keyName,
        replicationConfig, value.getBytes(UTF_8));
  }

  private void checkExceptedResultForVersioningTest(String volumeName,
      String bucketName, String keyName, int expectedCount) throws Exception {
    OMMetadataManager metadataManager = cluster.getOzoneManager().getMetadataManager();
    String ozoneKey = metadataManager.getOzoneKey(volumeName, bucketName, keyName);

    OmKeyInfo omKeyInfo = metadataManager.getKeyTable(VERSIONING_TEST_BUCKET_LAYOUT).get(ozoneKey);

    assertNotNull(omKeyInfo);
    assertEquals(expectedCount, omKeyInfo.getKeyLocationVersions().size());

    // Suspend KeyDeletingService to prevent it from purging entries from deleted table
    cluster.getOzoneManager().getKeyManager().getDeletingService().suspend();
    // ensure flush double buffer for deleted Table
    cluster.getOzoneManager().awaitDoubleBufferFlush();

    if (expectedCount == 1) {
      List<? extends Table.KeyValue<String, RepeatedOmKeyInfo>> rangeKVs
          = metadataManager.getDeletedTable().getRangeKVs(null, 100, ozoneKey);

      assertThat(rangeKVs).isNotEmpty();
      assertEquals(expectedCount,
          rangeKVs.get(0).getValue().getOmKeyInfoList().size());
    } else {
      // If expectedCount is greater than 1 means versioning enabled,
      // so delete table should be empty.
      RepeatedOmKeyInfo repeatedOmKeyInfo =
          metadataManager.getDeletedTable().get(ozoneKey);

      assertNull(repeatedOmKeyInfo);
    }
    cluster.getOzoneManager().getKeyManager().getDeletingService().resume();
  }

  @Test
  public void testOverWriteKeyWithAndWithOutVersioning() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    createRequiredForVersioningTest(volumeName, bucketName, keyName, false);

    checkExceptedResultForVersioningTest(volumeName, bucketName, keyName, 1);


    // Versioning turned on
    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    keyName = UUID.randomUUID().toString();

    createRequiredForVersioningTest(volumeName, bucketName, keyName, true);
    checkExceptedResultForVersioningTest(volumeName, bucketName, keyName, 2);
  }

  @Test
  public void testSetECReplicationConfigOnBucket()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    OzoneBucket bucket = getBucket(volume);
    ReplicationConfig currentReplicationConfig = bucket.getReplicationConfig();
    assertEquals(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE),
        currentReplicationConfig);
    ECReplicationConfig ecReplicationConfig =
        new ECReplicationConfig(3, 2, EcCodec.RS, (int) OzoneConsts.MB);
    bucket.setReplicationConfig(ecReplicationConfig);

    // Get the bucket and check the updated config.
    bucket = volume.getBucket(bucket.getName());

    assertEquals(ecReplicationConfig, bucket.getReplicationConfig());

    RatisReplicationConfig ratisReplicationConfig =
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
    bucket.setReplicationConfig(ratisReplicationConfig);

    // Get the bucket and check the updated config.
    bucket = volume.getBucket(bucket.getName());

    assertEquals(ratisReplicationConfig, bucket.getReplicationConfig());

    //Reset replication config back.
    bucket.setReplicationConfig(currentReplicationConfig);
  }

  private OzoneBucket getBucket(OzoneVolume volume) throws IOException {
    String bucketName = UUID.randomUUID().toString();
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setVersioning(true).setDefaultReplicationConfig(
        new DefaultReplicationConfig(
            StandaloneReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE)));
    volume.createBucket(bucketName, builder.build());
    return volume.getBucket(bucketName);
  }

  private static ReplicationConfig anyReplication() {
    return RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE);
  }

  private void assertBucketCount(OzoneVolume volume,
                                 String bucketPrefix,
                                 String preBucket,
                                 boolean hasSnapshot,
                                 int expectedBucketCount) {
    Iterator<? extends OzoneBucket> bucketIterator =
        volume.listBuckets(bucketPrefix, preBucket, hasSnapshot);
    int bucketCount = 0;
    while (bucketIterator.hasNext()) {
      assertTrue(
          bucketIterator.next().getName().startsWith(bucketPrefix));
      bucketCount++;
    }
    assertEquals(expectedBucketCount, bucketCount);
  }

  @Test
  public void testListSnapshot() throws IOException {
    String volumeA = "vol-a-" + RandomStringUtils.secure().nextNumeric(5);
    String volumeB = "vol-b-" + RandomStringUtils.secure().nextNumeric(5);
    String bucketA = "buc-a-" + RandomStringUtils.secure().nextNumeric(5);
    String bucketB = "buc-b-" + RandomStringUtils.secure().nextNumeric(5);
    store.createVolume(volumeA);
    store.createVolume(volumeB);
    OzoneVolume volA = store.getVolume(volumeA);
    OzoneVolume volB = store.getVolume(volumeB);
    volA.createBucket(bucketA);
    volA.createBucket(bucketB);
    volB.createBucket(bucketA);
    volB.createBucket(bucketB);
    String snapshotPrefixA = "snapshot-a-";
    String snapshotPrefixB = "snapshot-b-";
    for (int i = 0; i < 10; i++) {
      store.createSnapshot(volumeA, bucketA,
          snapshotPrefixA + i + "-" + RandomStringUtils.secure().nextNumeric(5));
      store.createSnapshot(volumeA, bucketB,
          snapshotPrefixA + i + "-" + RandomStringUtils.secure().nextNumeric(5));
      store.createSnapshot(volumeB, bucketA,
          snapshotPrefixA + i + "-" + RandomStringUtils.secure().nextNumeric(5));
      store.createSnapshot(volumeB, bucketB,
          snapshotPrefixA + i + "-" + RandomStringUtils.secure().nextNumeric(5));
    }
    for (int i = 0; i < 10; i++) {
      store.createSnapshot(volumeA, bucketA,
          snapshotPrefixB + i + "-" + RandomStringUtils.secure().nextNumeric(5));
      store.createSnapshot(volumeA, bucketB,
          snapshotPrefixB + i + "-" + RandomStringUtils.secure().nextNumeric(5));
      store.createSnapshot(volumeB, bucketA,
          snapshotPrefixB + i + "-" + RandomStringUtils.secure().nextNumeric(5));
      store.createSnapshot(volumeB, bucketB,
          snapshotPrefixB + i + "-" + RandomStringUtils.secure().nextNumeric(5));
    }

    Iterator<OzoneSnapshot> snapshotIter = store.listSnapshot(volumeA, bucketA, null, null);
    int volABucketASnapshotCount = 0;
    while (snapshotIter.hasNext()) {
      snapshotIter.next();
      volABucketASnapshotCount++;
    }
    assertEquals(20, volABucketASnapshotCount);

    snapshotIter = store.listSnapshot(volumeA, bucketB, null, null);
    int volABucketBSnapshotCount = 0;
    while (snapshotIter.hasNext()) {
      snapshotIter.next();
      volABucketBSnapshotCount++;
    }
    assertEquals(20, volABucketBSnapshotCount);

    snapshotIter = store.listSnapshot(volumeB, bucketA, null, null);
    int volBBucketASnapshotCount = 0;
    while (snapshotIter.hasNext()) {
      snapshotIter.next();
      volBBucketASnapshotCount++;
    }
    assertEquals(20, volBBucketASnapshotCount);

    snapshotIter = store.listSnapshot(volumeB, bucketB, null, null);
    int volBBucketBSnapshotCount = 0;
    while (snapshotIter.hasNext()) {
      snapshotIter.next();
      volBBucketBSnapshotCount++;
    }
    assertEquals(20, volBBucketBSnapshotCount);

    int volABucketASnapshotACount = 0;
    snapshotIter = store.listSnapshot(volumeA, bucketA, snapshotPrefixA, null);
    while (snapshotIter.hasNext()) {
      OzoneSnapshot snapshot = snapshotIter.next();
      assertTrue(snapshot.getName().startsWith(snapshotPrefixA));
      volABucketASnapshotACount++;
    }
    assertEquals(10, volABucketASnapshotACount);
    assertFalse(snapshotIter.hasNext());

  }

  /**
   * Tests get the information of key with network topology awareness enabled.
   */
  @Test
  void testGetKeyAndFileWithNetworkTopology() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = "sample value";
    getStore().createVolume(volumeName);
    OzoneVolume volume = getStore().getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();

    // Write data into a key
    TestDataUtil.createKey(bucket, keyName,
        ReplicationConfig.fromTypeAndFactor(RATIS, THREE),
        value.getBytes(UTF_8));

    // Since the rpc client is outside of cluster, then getFirstNode should be
    // equal to getClosestNode.
    OmKeyArgs.Builder builder = new OmKeyArgs.Builder();
    builder.setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName);

    // read key with topology aware read enabled
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] b = new byte[value.getBytes(UTF_8).length];
      IOUtils.readFully(is, b);
      assertArrayEquals(b, value.getBytes(UTF_8));
    }

    // read file with topology aware read enabled
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      byte[] b = new byte[value.getBytes(UTF_8).length];
      IOUtils.readFully(is, b);
      assertArrayEquals(b, value.getBytes(UTF_8));
    }

    // read key with topology aware read disabled
    OzoneConfiguration conf = getCluster().getConf();
    conf.setBoolean(OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY,
        false);
    try (OzoneClient newClient = OzoneClientFactory.getRpcClient(conf)) {
      ObjectStore newStore = newClient.getObjectStore();
      OzoneBucket newBucket =
          newStore.getVolume(volumeName).getBucket(bucketName);
      try (OzoneInputStream is = newBucket.readKey(keyName)) {
        byte[] b = new byte[value.getBytes(UTF_8).length];
        IOUtils.readFully(is, b);
        assertArrayEquals(b, value.getBytes(UTF_8));
      }

      // read file with topology aware read disabled
      try (OzoneInputStream is = newBucket.readFile(keyName)) {
        byte[] b = new byte[value.getBytes(UTF_8).length];
        IOUtils.readFully(is, b);
        assertArrayEquals(b, value.getBytes(UTF_8));
      }
    }
  }

  @ParameterizedTest
  @MethodSource("replicationConfigs")
  void testMultiPartUploadWithStream(ReplicationConfig replicationConfig)
      throws IOException, NoSuchAlgorithmException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = replicationConfig.getReplicationType().name().toLowerCase(Locale.ROOT) + "-bucket";
    String keyName = replicationConfig.getReplication();

    byte[] sampleData = new byte[1024 * 8];

    int valueLength = sampleData.length;

    getStore().createVolume(volumeName);
    OzoneVolume volume = getStore().getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        replicationConfig);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    assertNotNull(multipartInfo.getUploadID());

    OzoneDataStreamOutput ozoneStreamOutput = bucket.createMultipartStreamKey(
        keyName, valueLength, 1, uploadID);
    ozoneStreamOutput.write(ByteBuffer.wrap(sampleData), 0,
        valueLength);
    ozoneStreamOutput.getMetadata().put(OzoneConsts.ETAG,
        DatatypeConverter.printHexBinary(MessageDigest.getInstance(OzoneConsts.MD5_HASH)
            .digest(sampleData)).toLowerCase());
    ozoneStreamOutput.close();

    OzoneMultipartUploadPartListParts parts =
        bucket.listParts(keyName, uploadID, 0, 1);

    assertEquals(1, parts.getPartInfoList().size());

    OzoneMultipartUploadPartListParts.PartInfo partInfo =
        parts.getPartInfoList().get(0);
    assertEquals(valueLength, partInfo.getSize());

  }

  @Test
  public void testUploadWithStreamAndMemoryMappedBuffer(@TempDir Path dir) throws IOException {

    // create a local file
    final int chunkSize = 1024;
    final byte[] data = new byte[8 * chunkSize];
    ThreadLocalRandom.current().nextBytes(data);
    final File file = new File(dir.toString(), "data");
    try (OutputStream out = Files.newOutputStream(file.toPath())) {
      out.write(data);
    }

    // create a volume
    final String volumeName = "vol-" + UUID.randomUUID();
    getStore().createVolume(volumeName);
    final OzoneVolume volume = getStore().getVolume(volumeName);

    // create a bucket
    final String bucketName = "buck-" + UUID.randomUUID();
    final BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setDefaultReplicationConfig(
            new DefaultReplicationConfig(ReplicationConfig.fromTypeAndFactor(
                ReplicationType.RATIS, THREE)))
        .build();
    volume.createBucket(bucketName, bucketArgs);
    final OzoneBucket bucket = volume.getBucket(bucketName);

    // upload a key from the local file using memory-mapped buffers
    final String keyName = "key-" + UUID.randomUUID();
    try (RandomAccessFile raf = new RandomAccessFile(file, "r");
         OzoneDataStreamOutput out = bucket.createStreamKey(
             keyName, data.length)) {
      final FileChannel channel = raf.getChannel();
      long off = 0;
      for (long len = raf.length(); len > 0;) {
        final long writeLen = Math.min(len, chunkSize);
        final ByteBuffer mapped = channel.map(FileChannel.MapMode.READ_ONLY,
            off, writeLen);
        out.write(mapped);
        off += writeLen;
        len -= writeLen;
      }
    }

    // verify the key details
    final OzoneKeyDetails keyDetails = bucket.getKey(keyName);
    assertEquals(keyName, keyDetails.getName());
    assertEquals(data.length, keyDetails.getDataSize());

    // verify the key content
    final byte[] buffer = new byte[data.length];
    try (OzoneInputStream in = keyDetails.getContent()) {
      for (int off = 0; off < data.length;) {
        final int n = in.read(buffer, off, data.length - off);
        if (n < 0) {
          break;
        }
        off += n;
      }
    }
    assertArrayEquals(data, buffer);
  }

  @Test
  @Unhealthy("HDDS-10886")
  public void testParallelDeleteBucketAndCreateKey() throws IOException,
      InterruptedException, TimeoutException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = "sample value";
    getStore().createVolume(volumeName);
    OzoneVolume volume = getStore().getVolume(volumeName);
    volume.createBucket(bucketName);
    String keyName = UUID.randomUUID().toString();

    LogCapturer omSMLog = LogCapturer.captureLogs(OzoneManagerStateMachine.class);
    OzoneManagerStateMachine omSM = getCluster().getOzoneManager()
        .getOmRatisServer().getOmStateMachine();

    Thread thread1 = new Thread(() -> {
      try {
        volume.deleteBucket(bucketName);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    Thread thread2 = new Thread(() -> {
      try {
        getClient().getProxy().createKey(volumeName, bucketName, keyName,
            0, ReplicationType.RATIS, ONE, new HashMap<>());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    OMRequestHandlerPauseInjector injector =
        new OMRequestHandlerPauseInjector();
    omSM.getHandler().setInjector(injector);
    thread1.start();
    thread2.start();
    // Wait long enough for createKey's preExecute to finish executing
    GenericTestUtils.waitFor(() -> {
      return getCluster().getOzoneManager().getOmServerProtocol().getLastRequestToSubmit().getCmdType().equals(
          OzoneManagerProtocolProtos.Type.CreateKey);
    }, 100, 10000);
    injector.resume();

    try {
      thread1.join();
      thread2.join();
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }

    omSM.getHandler().setInjector(null);
    // Generate more write requests to OM
    String newBucketName = UUID.randomUUID().toString();
    volume.createBucket(newBucketName);
    OzoneBucket bucket = volume.getBucket(newBucketName);
    for (int i = 0; i < 10; i++) {
      bucket.createKey("key-" + i, value.getBytes(UTF_8).length,
          ReplicationType.RATIS, ONE, new HashMap<>());
    }

    assertThat(omSMLog.getOutput()).contains("Failed to write, Exception occurred");
  }

  @Test
  public void testGetServerDefaults() throws IOException {
    assertNotNull(getClient().getProxy().getServerDefaults());
    assertNull(getClient().getProxy().getServerDefaults().getKeyProviderUri());
  }

  private static class OMRequestHandlerPauseInjector extends FaultInjector {
    private CountDownLatch ready;
    private CountDownLatch wait;

    OMRequestHandlerPauseInjector() {
      init();
    }

    @Override
    public void init() {
      this.ready = new CountDownLatch(1);
      this.wait = new CountDownLatch(1);
    }

    @Override
    public void pause() throws IOException {
      ready.countDown();
      try {
        wait.await();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void resume() {
      // Make sure injector pauses before resuming.
      try {
        ready.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("resume interrupted");
      }
      wait.countDown();
    }

    @Override
    public void reset() throws IOException {
      init();
    }
  }

  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testPutObjectTagging(BucketLayout bucketLayout) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs bucketArgs =
        BucketArgs.newBuilder().setBucketLayout(bucketLayout).build();
    volume.createBucket(bucketName, bucketArgs);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String keyName = UUID.randomUUID().toString();

    TestDataUtil.createKey(bucket, keyName,
        anyReplication(), value.getBytes(UTF_8));

    OzoneKey key = bucket.getKey(keyName);
    assertTrue(key.getTags().isEmpty());

    Map<String, String> tags = new HashMap<>();
    tags.put("tag-key-1", "tag-value-1");
    tags.put("tag-key-2", "tag-value-2");

    bucket.putObjectTagging(keyName, tags);

    OzoneKey updatedKey = bucket.getKey(keyName);
    assertEquals(tags.size(), updatedKey.getTags().size());
    assertEquals(key.getModificationTime(), updatedKey.getModificationTime());
    assertThat(updatedKey.getTags()).containsAllEntriesOf(tags);

    // Do another putObjectTagging, it should override the previous one
    Map<String, String> secondTags = new HashMap<>();
    secondTags.put("tag-key-3", "tag-value-3");

    bucket.putObjectTagging(keyName, secondTags);

    updatedKey = bucket.getKey(keyName);
    assertEquals(secondTags.size(), updatedKey.getTags().size());
    assertThat(updatedKey.getTags()).containsAllEntriesOf(secondTags);
    assertThat(updatedKey.getTags()).doesNotContainKeys("tag-key-1", "tag-key-2");

    if (bucketLayout.equals(BucketLayout.FILE_SYSTEM_OPTIMIZED)) {
      String dirKey = "dir1/";
      bucket.createDirectory(dirKey);
      OMException exception = assertThrows(OMException.class,
          () -> bucket.putObjectTagging(dirKey, tags));
      assertThat(exception.getResult()).isEqualTo(ResultCodes.NOT_SUPPORTED_OPERATION);
    }
  }

  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testDeleteObjectTagging(BucketLayout bucketLayout) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs bucketArgs =
        BucketArgs.newBuilder().setBucketLayout(bucketLayout).build();
    volume.createBucket(bucketName, bucketArgs);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String keyName = UUID.randomUUID().toString();

    Map<String, String> tags = new HashMap<>();
    tags.put("tag-key-1", "tag-value-1");
    tags.put("tag-key-2", "tag-value-2");

    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(UTF_8).length, anyReplication(), new HashMap<>(), tags);
    out.write(value.getBytes(UTF_8));
    out.close();

    OzoneKey key = bucket.getKey(keyName);
    assertFalse(key.getTags().isEmpty());

    bucket.deleteObjectTagging(keyName);

    OzoneKey updatedKey = bucket.getKey(keyName);
    assertEquals(0, updatedKey.getTags().size());
    assertEquals(key.getModificationTime(), updatedKey.getModificationTime());

    if (bucketLayout.equals(BucketLayout.FILE_SYSTEM_OPTIMIZED)) {
      String dirKey = "dir1/";
      bucket.createDirectory(dirKey);
      OMException exception = assertThrows(OMException.class,
          () -> bucket.deleteObjectTagging(dirKey));
      assertThat(exception.getResult()).isEqualTo(ResultCodes.NOT_SUPPORTED_OPERATION);
    }
  }

  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testGetObjectTagging(BucketLayout bucketLayout) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs bucketArgs =
        BucketArgs.newBuilder().setBucketLayout(bucketLayout).build();
    volume.createBucket(bucketName, bucketArgs);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String keyName = UUID.randomUUID().toString();

    Map<String, String> tags = new HashMap<>();
    tags.put("tag-key-1", "tag-value-1");
    tags.put("tag-key-2", "tag-value-2");

    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(UTF_8).length, anyReplication(), new HashMap<>(), tags);
    out.write(value.getBytes(UTF_8));
    out.close();

    OzoneKey key = bucket.getKey(keyName);
    assertEquals(tags.size(), key.getTags().size());

    Map<String, String> tagsRetrieved = bucket.getObjectTagging(keyName);

    assertEquals(tags.size(), tagsRetrieved.size());
    assertThat(tagsRetrieved).containsAllEntriesOf(tags);
  }

  @Test
  public void testCreateEmptyKeySkipBlockAllocation()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    getStore().createVolume(volumeName);
    OzoneVolume volume = getStore().getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    long initialAllocatedBlocks =
        getCluster().getStorageContainerManager().getPipelineManager()
            .getMetrics().getTotalNumBlocksAllocated();
    // Don't write anything - this is an empty file
    OzoneOutputStream out = bucket.createKey(keyName, 0);
    out.close();

    // createKey should skip block allocation if data size is 0
    long currentAllocatedBlocks =
        getCluster().getStorageContainerManager().getPipelineManager().getMetrics().getTotalNumBlocksAllocated();
    assertEquals(initialAllocatedBlocks, currentAllocatedBlocks);
  }

  @Test
  public void testCreateEmptyFileNotSkipBlockAllocation()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    getStore().createVolume(volumeName);
    OzoneVolume volume = getStore().getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    long initialAllocatedBlocks =
        getCluster().getStorageContainerManager().getPipelineManager().getMetrics().getTotalNumBlocksAllocated();
    // Don't write anything - this is an empty file
    OzoneOutputStream out = bucket.createFile(keyName, 0,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        false,
        false);
    out.close();

    // OM should call allocateBlock in OMFileCreateRequest regardless of data size
    long currentAllocatedBlocks =
        getCluster().getStorageContainerManager().getPipelineManager().getMetrics().getTotalNumBlocksAllocated();
    assertEquals(initialAllocatedBlocks + 1, currentAllocatedBlocks);
  }
}
