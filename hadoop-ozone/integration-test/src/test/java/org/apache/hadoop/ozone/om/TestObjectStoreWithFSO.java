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

import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NFS_LEASE_CONFLICT;
import static org.apache.ozone.test.OzoneTestBase.uniqueObjectName;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ozone.OzoneFileSystem;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.SelfSignedCertificate;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.nfs.NfsChildrenPage;
import org.apache.hadoop.ozone.nfs.NfsExportBinding;
import org.apache.hadoop.ozone.nfs.NfsFileHandle;
import org.apache.hadoop.ozone.nfs.NfsStatus;
import org.apache.hadoop.ozone.nfs.NfsWriteBuffer;
import org.apache.hadoop.ozone.nfs.gateway.NfsCallerAuthorizer;
import org.apache.hadoop.ozone.nfs.gateway.NfsGatewayServer;
import org.apache.hadoop.ozone.nfs.gateway.NfsGatewayService;
import org.apache.hadoop.ozone.nfs.gateway.NfsObjectReader;
import org.apache.hadoop.ozone.nfs.gateway.NfsTlsPeerInterceptor;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayGrpc;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.Caller;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CapabilityRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.CapabilityResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.GetattrRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.GetattrResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.LookupRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.LookupResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.MkdirRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.MkdirResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.ReadRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.ReadResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.ReaddirRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.ReaddirResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RemoveRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RemoveResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RenameRequest;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RenameResponse;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RequestContext;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsObjectInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsTransferRangeInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.service.NfsObjectIndexBootstrapService;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsTransferRangesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupNfsTransferRangesResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseRequest.Operation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCommitRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCreateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsFileLeaseRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsTransferRangeResponse;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedNfsObjectInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests to verify Object store with prefix enabled cases.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestObjectStoreWithFSO implements NonHATests.TestCase {
  private static final Path ROOT =
      new Path(OZONE_URI_DELIMITER);
  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private String volumeName;
  private String bucketName;
  private FileSystem fs;
  private OzoneClient client;

  @BeforeAll
  void init() throws Exception {
    conf = new OzoneConfiguration(cluster().getConf());
    cluster = cluster();
    client = cluster.newClient();
    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket = DataTestUtil
        .createVolumeAndBucket(client, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    volumeName = bucket.getVolumeName();
    bucketName = bucket.getName();

    String rootPath = String
        .format("%s://%s.%s/", OzoneConsts.OZONE_URI_SCHEME, bucket.getName(),
            bucket.getVolumeName());
    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);
    fs = FileSystem.get(conf);
  }

  @AfterEach
  public void tearDown() throws Exception {
    deleteRootDir();
  }

  /**
   * Cleanup files and directories.
   *
   * @throws IOException DB failure
   */
  protected void deleteRootDir() throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(ROOT);

    if (fileStatuses == null) {
      return;
    }
    deleteRootRecursively(fileStatuses);
    fileStatuses = fs.listStatus(ROOT);
    if (fileStatuses != null) {
      assertEquals(0, fileStatuses.length, "Delete root failed!");
    }
  }

  private void deleteRootRecursively(FileStatus[] fileStatuses)
      throws IOException {
    for (FileStatus fStatus : fileStatuses) {
      fs.delete(fStatus.getPath(), true);
    }
  }

  @Test
  public void testCreateKey() throws Exception {
    String parent = "a/b/c/";
    String file = uniqueObjectName("key");
    String key = parent + file;

    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    assertEquals(volumeName, ozoneVolume.getName());
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    assertEquals(bucketName, ozoneBucket.getName());

    Table<String, OmKeyInfo> openFileTable =
        cluster.getOzoneManager().getMetadataManager()
            .getOpenKeyTable(getBucketLayout());

    // before file creation
    verifyKeyInFileTable(openFileTable, file, 0, true);

    String data = "random data";
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(key,
            data.length(), ReplicationType.RATIS, ReplicationFactor.ONE,
            new HashMap<>());

    KeyOutputStream keyOutputStream =
            (KeyOutputStream) ozoneOutputStream.getOutputStream();
    long clientID = keyOutputStream.getClientID();

    OmDirectoryInfo dirPathC = getDirInfo(parent);
    assertNotNull(dirPathC, "Failed to find dir path: a/b/c");

    // after file creation
    verifyKeyInOpenFileTable(openFileTable, clientID, file,
            dirPathC.getObjectID(), false);

    ozoneOutputStream.write(data.getBytes(StandardCharsets.UTF_8), 0,
            data.length());
    ozoneOutputStream.close();

    Table<String, OmKeyInfo> fileTable =
        cluster.getOzoneManager().getMetadataManager()
            .getKeyTable(getBucketLayout());
    // After closing the file. File entry should be removed from openFileTable
    // and it should be added to fileTable.
    verifyKeyInFileTable(fileTable, file, dirPathC.getObjectID(), false);
    verifyKeyInOpenFileTable(openFileTable, clientID, file,
            dirPathC.getObjectID(), true);

    ozoneBucket.deleteKey(key);

    // after key delete
    verifyKeyInFileTable(fileTable, file, dirPathC.getObjectID(), true);
    verifyKeyInOpenFileTable(openFileTable, clientID, file,
            dirPathC.getObjectID(), true);
  }

  /**
   * Tests bucket deletion behaviour. Buckets should not be allowed to be
   * deleted if they contain files or directories under them.
   *
   * @throws Exception
   */
  @Test
  public void testDeleteBucketWithKeys() throws Exception {
    // Create temporary volume and bucket for this test.
    OzoneBucket testBucket = DataTestUtil
        .createVolumeAndBucket(client, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    String testVolumeName = testBucket.getVolumeName();
    String testBucketName = testBucket.getName();

    String parent = "a/b/c/";
    String file = uniqueObjectName("key");
    String key = parent + file;

    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume ozoneVolume = objectStore.getVolume(testVolumeName);
    assertEquals(ozoneVolume.getName(), testVolumeName);
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(testBucketName);
    assertEquals(ozoneBucket.getName(), testBucketName);

    Table<String, OmKeyInfo> openFileTable =
        cluster.getOzoneManager().getMetadataManager()
            .getOpenKeyTable(getBucketLayout());

    // before file creation
    verifyKeyInFileTable(openFileTable, file, 0, true);

    // Create a key.
    ozoneBucket.createKey(key, 10).close();
    assertFalse(cluster.getOzoneManager().getMetadataManager().isBucketEmpty(
        testVolumeName, testBucketName));
    // Try to delete the bucket while a key is present under it.
    assertThrows(IOException.class, () -> ozoneVolume.deleteBucket(testBucketName),
        "Bucket Deletion should fail, since bucket is not empty.");

    // Delete the key (this only deletes the file)
    ozoneBucket.deleteKey(key);
    assertFalse(cluster.getOzoneManager().getMetadataManager()
        .isBucketEmpty(testVolumeName, testBucketName));
    // Try to delete the bucket while intermediate dirs are present under it.
    assertThrows(IOException.class, () -> ozoneVolume.deleteBucket(testBucketName),
        "Bucket Deletion should fail, since bucket still contains intermediate directories");

    // Delete last level of directories.
    ozoneBucket.deleteDirectory(parent, true);
    assertFalse(cluster.getOzoneManager().getMetadataManager()
        .isBucketEmpty(testVolumeName, testBucketName));
    // Try to delete the bucket while dirs are present under it.
    assertThrows(IOException.class, () -> ozoneVolume.deleteBucket(testBucketName),
        "Bucket Deletion should fail, since bucket still contains intermediate directories");

    // Delete all the intermediate directories
    ozoneBucket.deleteDirectory("a/", true);
    assertTrue(cluster.getOzoneManager().getMetadataManager()
        .isBucketEmpty(testVolumeName, testBucketName));
    ozoneVolume.deleteBucket(testBucketName);
    // Cleanup the Volume.
    client.getObjectStore().deleteVolume(testVolumeName);
  }

  @Test
  public void testLookupKey() throws Exception {
    String parent = "a/b/c/";
    String fileName = uniqueObjectName("key");
    String key = parent + fileName;

    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    assertEquals(volumeName, ozoneVolume.getName());
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    assertEquals(bucketName, ozoneBucket.getName());

    Table<String, OmKeyInfo> openFileTable =
        cluster.getOzoneManager().getMetadataManager()
            .getOpenKeyTable(getBucketLayout());

    String data = "random data";
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(key,
            data.length(), ReplicationType.RATIS, ReplicationFactor.ONE,
            new HashMap<>());

    KeyOutputStream keyOutputStream =
            (KeyOutputStream) ozoneOutputStream.getOutputStream();
    long clientID = keyOutputStream.getClientID();

    OmDirectoryInfo dirPathC = getDirInfo(parent);
    assertNotNull(dirPathC, "Failed to find dir path: a/b/c");

    // after file creation
    verifyKeyInOpenFileTable(openFileTable, clientID, fileName,
            dirPathC.getObjectID(), false);

    ozoneOutputStream.write(data.getBytes(StandardCharsets.UTF_8), 0,
            data.length());

    // open key
    OMException ome =
        assertThrows(OMException.class, () -> ozoneBucket.getKey(key),
            "Should throw exception as fileName is not visible and its still open for writing!");
    // expected
    assertEquals(ome.getResult(), OMException.ResultCodes.KEY_NOT_FOUND);

    ozoneOutputStream.close();

    OzoneKeyDetails keyDetails = ozoneBucket.getKey(key);
    assertEquals(key, keyDetails.getName());

    Table<String, OmKeyInfo> fileTable =
        cluster.getOzoneManager().getMetadataManager()
            .getKeyTable(getBucketLayout());

    // When closing the key, entry should be removed from openFileTable
    // and it should be added to fileTable.
    verifyKeyInFileTable(fileTable, fileName, dirPathC.getObjectID(), false);
    verifyKeyInOpenFileTable(openFileTable, clientID, fileName,
            dirPathC.getObjectID(), true);

    ozoneBucket.deleteKey(key);

    // get deleted key
    ome = assertThrows(OMException.class, () -> ozoneBucket.getKey(key),
        "Should throw exception as fileName not exists!");
    // expected
    assertEquals(ome.getResult(), OMException.ResultCodes.KEY_NOT_FOUND);

    // after key delete
    verifyKeyInFileTable(fileTable, fileName, dirPathC.getObjectID(), true);
    verifyKeyInOpenFileTable(openFileTable, clientID, fileName,
            dirPathC.getObjectID(), true);
  }

  /**
   * Verify listKeys at different levels.
   *
   *                  buck-1
   *                    |
   *                    a
   *                    |
   *      --------------------------------------
   *     |              |                       |
   *     b1             b2                      b3
   *    -----           --------               ----------
   *   |      |        |    |   |             |    |     |
   *  c1     c2        d1   d2  d3             e1   e2   e3
   *  |      |         |    |   |              |    |    |
   * c1.tx  c2.tx   d11.tx  | d31.tx           |    |    e31.tx
   *                      --------             |   e21.tx
   *                     |        |            |
   *                   d21.tx   d22.tx        e11.tx
   *
   * Above is the FS tree structure.
   */
  @Test
  public void testListKeysAtDifferentLevels() throws Exception {
    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    assertEquals(volumeName, ozoneVolume.getName());
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    assertEquals(bucketName, ozoneBucket.getName());

    String keyc1 = "/a/b1/c1/c1.tx";
    String keyc2 = "/a/b1/c2/c2.tx";

    String keyd13 = "/a/b2/d1/d11.tx";
    String keyd21 = "/a/b2/d2/d21.tx";
    String keyd22 = "/a/b2/d2/d22.tx";
    String keyd31 = "/a/b2/d3/d31.tx";

    String keye11 = "/a/b3/e1/e11.tx";
    String keye21 = "/a/b3/e2/e21.tx";
    String keye31 = "/a/b3/e3/e31.tx";

    LinkedList<String> keys = new LinkedList<>();
    keys.add(keyc1);
    keys.add(keyc2);

    keys.add(keyd13);
    keys.add(keyd21);
    keys.add(keyd22);
    keys.add(keyd31);

    keys.add(keye11);
    keys.add(keye21);
    keys.add(keye31);

    int length = 10;
    byte[] input = new byte[length];
    Arrays.fill(input, (byte)96);

    createAndAssertKeys(ozoneBucket, keys);

    // Root level listing keys
    Iterator<? extends OzoneKey> ozoneKeyIterator =
        ozoneBucket.listKeys(null, null);
    verifyFullTreeStructure(ozoneKeyIterator);

    ozoneKeyIterator =
        ozoneBucket.listKeys("a/", null);
    verifyFullTreeStructure(ozoneKeyIterator);

    LinkedList<String> expectedKeys;

    // Intermediate level keyPrefix - 2nd level
    ozoneKeyIterator =
        ozoneBucket.listKeys("a///b2///", null);
    expectedKeys = new LinkedList<>();
    expectedKeys.add("a/b2/");
    expectedKeys.add("a/b2/d1/");
    expectedKeys.add("a/b2/d1/d11.tx");
    expectedKeys.add("a/b2/d2/");
    expectedKeys.add("a/b2/d2/d21.tx");
    expectedKeys.add("a/b2/d2/d22.tx");
    expectedKeys.add("a/b2/d3/");
    expectedKeys.add("a/b2/d3/d31.tx");
    checkKeyList(ozoneKeyIterator, expectedKeys);

    // Intermediate level keyPrefix - 3rd level
    // Without trailing slash
    ozoneKeyIterator =
        ozoneBucket.listKeys("a/b2/d1", null);
    expectedKeys = new LinkedList<>();
    expectedKeys.add("a/b2/d1/");
    expectedKeys.add("a/b2/d1/d11.tx");
    checkKeyList(ozoneKeyIterator, expectedKeys);
    // With trailing slash
    ozoneKeyIterator =
        ozoneBucket.listKeys("a/b2/d1/", null);
    checkKeyList(ozoneKeyIterator, expectedKeys);

    // Boundary of a level
    ozoneKeyIterator =
        ozoneBucket.listKeys("a/b2/d2", "a/b2/d2/d21.tx");
    expectedKeys = new LinkedList<>();
    expectedKeys.add("a/b2/d2/d22.tx");
    checkKeyList(ozoneKeyIterator, expectedKeys);

    // Boundary case - last node in the depth-first-traversal
    ozoneKeyIterator =
        ozoneBucket.listKeys("a/b3/e3", "a/b3/e3/e31.tx");
    expectedKeys = new LinkedList<>();
    checkKeyList(ozoneKeyIterator, expectedKeys);

    // Key level, prefix=key case
    ozoneKeyIterator =
        ozoneBucket.listKeys("a/b1/c1/c1.tx");
    expectedKeys = new LinkedList<>();
    expectedKeys.add("a/b1/c1/c1.tx");
    checkKeyList(ozoneKeyIterator, expectedKeys);

    // Key directly under bucket
    createTestKey(ozoneBucket, "key1.tx", "key1");
    ozoneKeyIterator =
        ozoneBucket.listKeys("key1.tx");
    expectedKeys = new LinkedList<>();
    expectedKeys.add("key1.tx");
    checkKeyList(ozoneKeyIterator, expectedKeys);
  }

  private void verifyFullTreeStructure(Iterator<? extends OzoneKey> keyItr) {
    LinkedList<String> expectedKeys = new LinkedList<>();
    expectedKeys.add("a/");
    expectedKeys.add("a/b1/");
    expectedKeys.add("a/b1/c1/");
    expectedKeys.add("a/b1/c1/c1.tx");
    expectedKeys.add("a/b1/c2/");
    expectedKeys.add("a/b1/c2/c2.tx");
    expectedKeys.add("a/b2/");
    expectedKeys.add("a/b2/d1/");
    expectedKeys.add("a/b2/d1/d11.tx");
    expectedKeys.add("a/b2/d2/");
    expectedKeys.add("a/b2/d2/d21.tx");
    expectedKeys.add("a/b2/d2/d22.tx");
    expectedKeys.add("a/b2/d3/");
    expectedKeys.add("a/b2/d3/d31.tx");
    expectedKeys.add("a/b3/");
    expectedKeys.add("a/b3/e1/");
    expectedKeys.add("a/b3/e1/e11.tx");
    expectedKeys.add("a/b3/e2/");
    expectedKeys.add("a/b3/e2/e21.tx");
    expectedKeys.add("a/b3/e3/");
    expectedKeys.add("a/b3/e3/e31.tx");
    checkKeyList(keyItr, expectedKeys);
  }

  @Test
  public void testListKeysWithNotNormalizedPath() throws Exception {
    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    assertEquals(volumeName, ozoneVolume.getName());
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    assertEquals(bucketName, ozoneBucket.getName());

    String key1 = "/dir1///dir2/file1/";
    String key2 = "/dir1///dir2/file2/";
    String key3 = "/dir1///dir2/file3/";

    LinkedList<String> keys = new LinkedList<>();
    keys.add("dir1/");
    keys.add("dir1/dir2/");
    keys.add(OmUtils.normalizeKey(key1, false));
    keys.add(OmUtils.normalizeKey(key2, false));
    keys.add(OmUtils.normalizeKey(key3, false));

    createAndAssertKeys(ozoneBucket, Arrays.asList(key1, key2, key3));

    // Iterator with key name as prefix.
    Iterator<? extends OzoneKey> ozoneKeyIterator =
            ozoneBucket.listKeys("/dir1//", null);

    checkKeyList(ozoneKeyIterator, keys);

    // Iterator with with normalized key prefix.
    ozoneKeyIterator =
            ozoneBucket.listKeys("dir1/");

    checkKeyList(ozoneKeyIterator, keys);

    // Iterator with key name as previous key.
    ozoneKeyIterator = ozoneBucket.listKeys(null,
            "/dir1///dir2/file1/");

    // Remove keys before //dir1/dir2/file1
    keys.remove("dir1/");
    keys.remove("dir1/dir2/");
    keys.remove("dir1/dir2/file1");

    checkKeyList(ozoneKeyIterator, keys);

    // Iterator with  normalized key as previous key.
    ozoneKeyIterator = ozoneBucket.listKeys(null,
            OmUtils.normalizeKey(key1, false));

    checkKeyList(ozoneKeyIterator, keys);
  }

  private void checkKeyList(Iterator<? extends OzoneKey > ozoneKeyIterator,
      List<String> keys) {

    LinkedList<String> outputKeys = new LinkedList<>();
    while (ozoneKeyIterator.hasNext()) {
      OzoneKey ozoneKey = ozoneKeyIterator.next();
      outputKeys.add(ozoneKey.getName());
    }

    assertEquals(keys, outputKeys);
  }

  private void createAndAssertKeys(OzoneBucket ozoneBucket, List<String> keys)
      throws Exception {

    for (String key : keys) {
      byte[] input = DataTestUtil.createStringKey(ozoneBucket, key, 10);
      // Read the key with given key name.
      readKey(ozoneBucket, key, 10, input);
    }
  }

  private void readKey(OzoneBucket ozoneBucket, String key, int length, byte[] input)
      throws Exception {

    byte[] read = new byte[length];
    try (InputStream ozoneInputStream = ozoneBucket.readKey(key)) {
      IOUtils.readFully(ozoneInputStream, read);
    }

    String inputString = new String(input, StandardCharsets.UTF_8);
    assertEquals(inputString, new String(read, StandardCharsets.UTF_8));

    // Read using filesystem.
    String rootPath = String.format("%s://%s.%s/", OZONE_URI_SCHEME,
        bucketName, volumeName);
    OzoneFileSystem o3fs = (OzoneFileSystem) FileSystem.get(new URI(rootPath),
        conf);
    try (InputStream fsDataInputStream = o3fs.open(new Path(key))) {
      IOUtils.readFully(fsDataInputStream, read);
    }

    assertEquals(inputString, new String(read, StandardCharsets.UTF_8));
  }

  @Test
  @Tag("nfs")
  public void testNfsFileIdentityContract(@TempDir java.nio.file.Path nfsTempDir) throws Exception {
    OzoneBucket bucket = client.getObjectStore().getVolume(volumeName).getBucket(bucketName);
    UUID exportId = UUID.randomUUID();
    String original = "nfs/source/file";
    String renamed = "nfs/source/renamed";
    String moved = "nfs/moved/renamed";
    createTestKey(bucket, original, "NFS identity contract");
    NfsFileHandle handle = nfsFileHandle(exportId, original);
    long directoryId = getDirInfo("nfs/source").getObjectID();
    OMMetadataManager metadata = cluster.getOzoneManager().getMetadataManager();
    assertTrue(metadata.isNfsObjectIndexEnabled());
    assertFalse(metadata.isNfsObjectIndexReady(), "Index maintenance alone must not enable NFS lookup");
    OmDirectoryInfo parent = getDirInfo("nfs");
    assertNfsObjectIndexEntries(metadata, handle, directoryId, parent.getObjectID());

    OmKeyArgs identityArgs = new OmKeyArgs.Builder().setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName("").build();
    OzoneManagerProtocol rpc =
        ((RpcClient) client.getObjectStore().getClientProxy()).getOzoneManagerClient();
    assertEquals(OMException.ResultCodes.OM_NOT_INITIALIZED, assertThrows(OMException.class,
        () -> rpc.lookupNfsObject(identityArgs, handle.getVolumeId(), handle.getBucketId(), handle.getObjectId(),
            NfsFileHandle.Kind.FILE)).getResult());

    NfsObjectIndexBootstrapService bootstrap = new NfsObjectIndexBootstrapService(
        20, TimeUnit.MILLISECONDS, cluster.getOzoneManager());
    try {
      bootstrap.start();
      GenericTestUtils.waitFor(() -> {
        try {
          return metadata.isNfsObjectIndexReady();
        } catch (IOException ex) {
          fail("Cannot read NFS index readiness", ex);
          return false;
        }
      }, 20, 30000);
      assertEquals("DONE", metadata.getMetaTable().getSkipCache(OzoneConsts.NFS_OBJECT_INDEX_PROGRESS_KEY));
    } finally {
      bootstrap.shutdown();
    }
    if (!metadata.isNfsWriterLeaseEnabled() || !metadata.isNfsDurableWriteRangesEnabled()) {
      cluster.getOzoneManager().finalizeUpgrade();
    }
    assertNfsExportLeaseRpc(metadata, rpc, exportId, volumeName, bucketName);

    assertNfsClientWrite(bucket, rpc, metadata, exportId, handle);
    // Transport authentication is outside this non-secure gateway-to-OM RPC contract.
    RequestContext callerContext = RequestContext.newBuilder().setVersion(1)
        .setExportUuid(ByteString.copyFrom(ByteBuffer.allocate(16)
            .putLong(exportId.getMostSignificantBits()).putLong(exportId.getLeastSignificantBits()).array()))
        .setCaller(Caller.newBuilder().setKerberosPrincipal("nfs-test@EXAMPLE.COM")
            .setProtection(Caller.Protection.PRIVACY))
        .build();
    NfsCallerAuthorizer callerAuthorizer = mock(NfsCallerAuthorizer.class);
    when(callerAuthorizer.authorize(callerContext)).thenReturn(UserGroupInformation.getCurrentUser());
    NfsExportBinding export = new NfsExportBinding(exportId, cluster.getClusterId(), volumeName, bucketName,
        handle.getVolumeId(), handle.getBucketId());
    NfsObjectReader gatewayReader = new NfsObjectReader(conf, null, cluster.getClusterId(), export, callerAuthorizer);
    ByteString encodedHandle = ByteString.copyFrom(handle.toBytes());
    assertNfsTlsTransportReadsOzone(nfsTempDir, gatewayReader, callerContext, encodedHandle, exportId);
    assertNfsGatewayServiceContracts(gatewayReader, callerContext, bucket, encodedHandle, exportId,
        handle.getVolumeId(), handle.getBucketId());
    String ecKey = "nfs/source/ec-file";
    byte[] ecData = "NFS EC read contract".getBytes(StandardCharsets.UTF_8);
    DataTestUtil.createKey(bucket, ecKey, new ECReplicationConfig("RS-3-2-1024k"), ecData);
    ByteString encodedEcHandle = ByteString.copyFrom(nfsFileHandle(exportId, ecKey).toBytes());
    assertArrayEquals(ecData, gatewayReader.read(callerContext, encodedEcHandle, 0, ecData.length).getData()
        .toByteArray(), "NFS ranged reads must support EC-backed Ozone keys");
    bucket.deleteKey(ecKey);

    assertEquals(handle.getObjectId(), rpc.lookupNfsObject(identityArgs, handle.getVolumeId(), handle.getBucketId(),
        handle.getObjectId(), NfsFileHandle.Kind.FILE).getKeyInfo().getObjectID());
    OmKeyInfo readable = rpc.lookupNfsObject(identityArgs, handle.getVolumeId(), handle.getBucketId(),
        handle.getObjectId(), NfsFileHandle.Kind.FILE).getKeyInfo();
    assertTrue(readable.getLatestVersionLocations().getLocationListCount() > 0);
    OmKeyInfo head = rpc.lookupNfsObject(identityArgs.toBuilder().setHeadOp(true).build(), handle.getVolumeId(),
        handle.getBucketId(), handle.getObjectId(), NfsFileHandle.Kind.FILE).getKeyInfo();
    assertTrue(head.getKeyLocationVersions().isEmpty(), "Head lookup must omit block locations");
    assertTrue(rpc.lookupNfsObject(identityArgs, handle.getVolumeId(), handle.getBucketId(), handle.getBucketId(),
        NfsFileHandle.Kind.BUCKET).isDirectory());
    assertEquals(directoryId, rpc.lookupNfsObject(identityArgs, handle.getVolumeId(), handle.getBucketId(), directoryId,
        NfsFileHandle.Kind.DIRECTORY).getKeyInfo().getObjectID());

    assertNfsDirectoryCursor(bucket, rpc, identityArgs, handle.getVolumeId(), handle.getBucketId());

    for (int i = 0; i < 2; i++) {
      byte[] data = ("Multipart replacement " + i).getBytes(StandardCharsets.UTF_8);
      OmMultipartInfo upload = bucket.initiateMultipartUpload(original,
          RatisReplicationConfig.getInstance(ONE.toProto()));
      OzoneOutputStream part = bucket.createMultipartKey(original, data.length, 1, upload.getUploadID());
      try (OzoneOutputStream output = part) {
        output.write(data);
        output.getMetadata().put(OzoneConsts.ETAG, MD5Hash.digest(data).toString());
      }
      bucket.completeMultipartUpload(original, upload.getUploadID(),
          Collections.singletonMap(1, part.getCommitUploadPartInfo().getETag()));
      assertEquals(handle, nfsFileHandle(exportId, original), "Multipart overwrite must preserve existing identity");
      assertEquals(new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, directoryId, "file"),
          metadata.getNfsObjectTable().get(metadata.getNfsObjectKey(handle.getVolumeId(), handle.getBucketId(),
              handle.getObjectId())));
      try (InputStream input = bucket.readKey(original)) {
        assertArrayEquals(data, IOUtils.toByteArray(input));
      }
    }

    bucket.renameKey(original, renamed);
    assertEquals(handle, nfsFileHandle(exportId, renamed), "File rename must preserve NFS identity");
    assertEquals(new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, directoryId, "renamed"),
        metadata.getNfsObjectTable().get(metadata.getNfsObjectKey(handle.getVolumeId(), handle.getBucketId(),
            handle.getObjectId())));
    assertTrue(fs.rename(new Path("/nfs/source"), new Path("/nfs/moved")));
    assertEquals(directoryId, getDirInfo("nfs/moved").getObjectID(), "Directory rename must preserve its identity");
    assertEquals(new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.DIRECTORY, parent.getObjectID(), "moved"),
        metadata.getNfsObjectTable().get(metadata.getNfsObjectKey(handle.getVolumeId(), handle.getBucketId(),
            directoryId)));
    assertEquals(new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, directoryId, "renamed"),
        metadata.getNfsObjectTable().get(metadata.getNfsObjectKey(handle.getVolumeId(), handle.getBucketId(),
            handle.getObjectId())));
    assertArrayEquals(handle.toBytes(), nfsFileHandle(exportId, moved).toBytes(),
        "Ancestor rename must preserve descendant NFS identity");
    assertEquals(handle, NfsFileHandle.fromBytes(handle.toBytes()));

    assertEquals(handle.getObjectId(), rpc.lookupNfsObject(identityArgs, handle.getVolumeId(), handle.getBucketId(),
        handle.getObjectId(), NfsFileHandle.Kind.FILE).getKeyInfo().getObjectID());

    assertEquals(moved, gatewayReader.lookup(callerContext, encodedHandle, true).getKeyInfo().getKeyName());
    assertEquals("Multipart replacement 1", gatewayReader.read(callerContext, encodedHandle, 0, 64).getData()
        .toStringUtf8());

    assertNfsObjectStaleAfterDelete(bucket, metadata, exportId, handle, moved);
    assertNfsStaleGatewayLookups(rpc, identityArgs, handle, gatewayReader, callerContext, encodedHandle);
    assertNfsObjectCleanup(bucket, metadata, nfsFileHandle(exportId, moved), handle, moved, directoryId, fs);
  }

  private void assertNfsGatewayServiceContracts(NfsObjectReader gatewayReader, RequestContext callerContext,
      OzoneBucket bucket, ByteString encodedHandle, UUID exportId, long volumeId, long bucketId) throws Exception {
    assertNfsReadRanges(gatewayReader, callerContext, encodedHandle, bucket, exportId);
    assertNfsNamespaceMutations(gatewayReader, callerContext, bucket, exportId, volumeId, bucketId);
  }

  private void assertNfsTlsTransportReadsOzone(java.nio.file.Path tempDir, NfsObjectReader reader,
      RequestContext callerContext, ByteString encodedHandle, UUID exportId) throws Exception {
    SecurityConfig securityConfig = new SecurityConfig(conf);
    KeyPair keyPair = new HDDSKeyGenerator(securityConfig).generateKey();
    X509Certificate certificate = SelfSignedCertificate.newBuilder().setSubject("localhost")
        .setClusterID(cluster.getClusterId()).setScmID("nfs-gateway-it")
        .setBeginDate(ZonedDateTime.now().minusDays(1)).setEndDate(ZonedDateTime.now().plusDays(1))
        .setConfiguration(securityConfig).setKey(keyPair).addDnsName("localhost").build();
    File certificateFile = tempDir.resolve("nfs-gateway.crt").toFile();
    File privateKeyFile = tempDir.resolve("nfs-gateway.key").toFile();
    Files.write(certificateFile.toPath(), CertificateCodec.getPEMEncodedString(certificate)
        .getBytes(StandardCharsets.US_ASCII));
    Files.write(privateKeyFile.toPath(), securityConfig.keyCodec().encodePrivateKey(keyPair.getPrivate()));
    try (NfsGatewayServer server = new NfsGatewayServer(new InetSocketAddress("127.0.0.1", 0), certificateFile,
        privateKeyFile, certificateFile, Map.of(NfsTlsPeerInterceptor.fingerprint(certificate), Set.of(exportId)),
        new NfsGatewayService(reader))) {
      ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
          .overrideAuthority("localhost")
          .sslContext(GrpcSslContexts.forClient().keyManager(certificateFile, privateKeyFile)
              .trustManager(certificateFile).build())
          .build();
      try {
        NfsGatewayGrpc.NfsGatewayBlockingStub stub = NfsGatewayGrpc.newBlockingStub(channel);
        CapabilityResponse capabilities = stub.capabilities(CapabilityRequest.newBuilder().setVersion(1)
            .setExportUuid(callerContext.getExportUuid()).build());
        assertEquals(NfsStatus.OK.getCode(), capabilities.getResult().getNfsStatus());
        assertTrue(capabilities.getOperationsList().contains(CapabilityResponse.Operation.READ));
        NfsFileHandle fileHandle = NfsFileHandle.fromBytes(encodedHandle.toByteArray());
        ByteString rootHandle = ByteString.copyFrom(new NfsFileHandle(exportId, fileHandle.getVolumeId(),
            fileHandle.getBucketId(), fileHandle.getBucketId(), NfsFileHandle.Kind.BUCKET).toBytes());
        GetattrResponse attributes = stub.getattr(GetattrRequest.newBuilder().setContext(callerContext)
            .setHandle(encodedHandle).build());
        assertEquals(NfsStatus.OK.getCode(), attributes.getResult().getNfsStatus());
        assertEquals(fileHandle.getObjectId(), attributes.getAttributes().getObjectId());
        LookupResponse lookup = stub.lookup(LookupRequest.newBuilder().setContext(callerContext)
            .setParentHandle(rootHandle).setName("nfs").build());
        assertEquals(NfsStatus.OK.getCode(), lookup.getResult().getNfsStatus());
        assertTrue(lookup.getAttributes().getDirectory());
        ReaddirResponse readdir = stub.withDeadlineAfter(30, TimeUnit.SECONDS).readdir(
            ReaddirRequest.newBuilder().setContext(callerContext).setDirectoryHandle(rootHandle)
                .setMaxEntries(16).build());
        assertEquals(NfsStatus.OK.getCode(), readdir.getResult().getNfsStatus());
        assertTrue(readdir.getEntriesList().stream().anyMatch(entry -> "nfs".equals(entry.getName())));
        ReadResponse response = stub.read(ReadRequest.newBuilder().setContext(callerContext).setHandle(encodedHandle)
            .setOffset(0).setCount(64).build());
        assertEquals(NfsStatus.OK.getCode(), response.getResult().getNfsStatus());
        assertEquals("NFS identity contract", response.getData().toStringUtf8());
        assertTrue(response.getEof());
      } finally {
        channel.shutdownNow();
      }
    }
  }

  private void assertNfsObjectStaleAfterDelete(OzoneBucket bucket, OMMetadataManager metadata, UUID exportId,
      NfsFileHandle handle, String moved) throws Exception {
    bucket.deleteKey(moved);
    assertNull(metadata.getNfsObjectTable().get(metadata.getNfsObjectKey(handle.getVolumeId(), handle.getBucketId(),
        handle.getObjectId())));
    OMException deleted = assertThrows(OMException.class, () -> nfsFileHandle(exportId, moved));
    assertEquals(FILE_NOT_FOUND, deleted.getResult());
    assertEquals(NfsStatus.NOENT, NfsStatus.fromOmException(deleted, NfsStatus.LookupContext.NAME));
    assertEquals(NfsStatus.STALE, NfsStatus.fromOmException(deleted, NfsStatus.LookupContext.HANDLE));
    OMException directory = assertThrows(OMException.class, () -> nfsFileHandle(exportId, "nfs/moved"));
    assertEquals(NfsStatus.ISDIR, NfsStatus.fromOmException(directory, NfsStatus.LookupContext.HANDLE));
    createTestKey(bucket, moved, "Replacement file");
    assertNotEquals(handle, nfsFileHandle(exportId, moved), "Recreation must not reuse the deleted file identity");
  }

  private void assertNfsStaleGatewayLookups(OzoneManagerProtocol rpc, OmKeyArgs identityArgs, NfsFileHandle handle,
      NfsObjectReader gatewayReader, RequestContext callerContext, ByteString encodedHandle) throws Exception {
    OMException stale = assertThrows(OMException.class, () -> rpc.lookupNfsObject(identityArgs,
        handle.getVolumeId(), handle.getBucketId(), handle.getObjectId(), NfsFileHandle.Kind.FILE));
    assertEquals(FILE_NOT_FOUND, stale.getResult());
    assertEquals(NfsStatus.STALE, NfsStatus.fromOmException(stale, NfsStatus.LookupContext.HANDLE));
    assertEquals(FILE_NOT_FOUND, assertThrows(OMException.class,
        () -> gatewayReader.lookup(callerContext, encodedHandle, true)).getResult());
    assertEquals(FILE_NOT_FOUND, assertThrows(OMException.class,
        () -> gatewayReader.read(callerContext, encodedHandle, 0, 4)).getResult());
  }

  private void assertNfsObjectCleanup(OzoneBucket bucket, OMMetadataManager metadata, NfsFileHandle replacement,
      NfsFileHandle handle, String moved, long directoryId, FileSystem fileSystem) throws Exception {
    bucket.deleteKeys(Collections.singletonList(moved));
    assertNull(metadata.getNfsObjectTable().get(metadata.getNfsObjectKey(replacement.getVolumeId(),
        replacement.getBucketId(), replacement.getObjectId())));
    assertTrue(fileSystem.delete(new Path("/nfs/moved"), false));
    assertNull(metadata.getNfsObjectTable().get(metadata.getNfsObjectKey(handle.getVolumeId(), handle.getBucketId(),
        directoryId)));
  }

  private void assertNfsClientWrite(OzoneBucket bucket, OzoneManagerProtocol rpc, OMMetadataManager metadata,
      UUID exportId, NfsFileHandle existingHandle) throws Exception {
    NfsExportLeaseRequest acquire = NfsExportLeaseRequest.newBuilder().setOperation(Operation.ACQUIRE)
        .setExportUUID(exportId.toString()).setVolumeName(volumeName).setBucketName(bucketName)
        .setGatewayID("nfs-write-it").setLeaseDurationMillis(30_000).build();
    org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseResponse exportLease =
        rpc.updateNfsExportLease(acquire);
    String transferId = UUID.randomUUID().toString();
    NfsFileCreateRequest createRequest = NfsFileCreateRequest.newBuilder().setExportUUID(exportId.toString())
        .setGatewayID("nfs-write-it").setGatewayGeneration(exportLease.getGatewayGeneration())
        .setTransferID(transferId).setLeaseDurationMillis(10_000).build();
    String key = "nfs/client-write";
    byte[] data = "NFS write through the OM lease".getBytes(StandardCharsets.UTF_8);
    int split = data.length / 2;
    NfsWriteBuffer writeBuffer = new NfsWriteBuffer(1024, 4);
    RpcClient.NfsFileOutput nfsOutput = ((RpcClient) client.getObjectStore().getClientProxy())
        .createNfsFileWithLease(volumeName, bucketName, key, data.length,
            RatisReplicationConfig.getInstance(ONE.toProto()), createRequest);
    NfsFileCreateResponse createLease = nfsOutput.getLease();
    try (OzoneOutputStream output = nfsOutput.getOutputStream()) {
      assertEquals(0, writeBuffer.offer(split, Arrays.copyOfRange(data, split, data.length)).length);
      byte[] contiguous = writeBuffer.offer(0, Arrays.copyOfRange(data, 0, split));
      assertArrayEquals(data, contiguous);
      output.write(contiguous);
      writeBuffer.acknowledgeWritten(contiguous.length);
    }
    NfsFileHandle writtenHandle = nfsFileHandle(exportId, key);
    assertEquals(writtenHandle.getObjectId(), createLease.getObjectID());
    String fileLeaseKey = metadata.getNfsFileLeaseKey(
        existingHandle.getVolumeId(), existingHandle.getBucketId(), writtenHandle.getObjectId());
    OmNfsLeaseInfo fileLease = metadata.getNfsFileLeaseTable().get(fileLeaseKey);
    assertNotNull(fileLease, "NFS file lease must be indexed by the published file identity");
    assertFalse(fileLease.getOwnerPrincipal().isEmpty(), "NFS file lease must persist the authenticated owner");
    assertEquals(createLease.getGeneration(), fileLease.getProtobuf().getGeneration());
    assertEquals(createLease.getOwnerToken(), fileLease.getProtobuf().getOwnerToken());
    assertTrue(fileLease.getProtobuf().getExpiresAtMillis() > System.currentTimeMillis());
    NfsFileLeaseRequest renewFileLease = NfsFileLeaseRequest.newBuilder()
        .setOperation(NfsFileLeaseRequest.Operation.RENEW).setExportUUID(exportId.toString())
        .setVolumeName(volumeName).setBucketName(bucketName).setObjectID(writtenHandle.getObjectId())
        .setGatewayID("nfs-write-it").setGatewayGeneration(exportLease.getGatewayGeneration())
        .setTransferID(fileLease.getProtobuf().getTransferID()).setGeneration(fileLease.getProtobuf().getGeneration())
        .setOwnerToken(fileLease.getProtobuf().getOwnerToken()).setLeaseDurationMillis(10_000).build();
    assertTrue(rpc.updateNfsFileLease(renewFileLease).getExpiresAtMillis()
        > fileLease.getProtobuf().getExpiresAtMillis());
    String stagingBucketName = "nfs-staging-" + UUID.randomUUID().toString().replace("-", "");
    OzoneVolume volume = client.getObjectStore().getVolume(volumeName);
    BucketArgs stagingBucketArgs = BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED).build();
    volume.createBucket(stagingBucketName, stagingBucketArgs);
    OzoneBucket stagingBucket = volume.getBucket(stagingBucketName);
    String stagingKey = exportId + "/" + transferId + "/00000000000000000000";
    try (OzoneOutputStream output = stagingBucket.createKey(stagingKey, data.length, RATIS, ONE, new HashMap<>())) {
      output.write(data);
    }
    byte[] digest = java.security.MessageDigest.getInstance("SHA-256").digest(data);
    NfsTransferRangeRequest rangeRequest = NfsTransferRangeRequest.newBuilder()
        .setOperation(NfsTransferRangeRequest.Operation.UPSERT).setVolumeName(volumeName).setBucketName(bucketName)
        .setExportUUID(exportId.toString()).setObjectID(writtenHandle.getObjectId()).setGatewayID("nfs-write-it")
        .setGatewayGeneration(exportLease.getGatewayGeneration()).setTransferID(transferId)
        .setLeaseGeneration(fileLease.getProtobuf().getGeneration())
        .setOwnerToken(fileLease.getProtobuf().getOwnerToken()).setOffset(0)
        .setLength(data.length).setStagingKey(volumeName + "/" + stagingBucketName + "/" + stagingKey)
        .setSha256(ByteString.copyFrom(digest)).build();
    NfsTransferRangeResponse firstRange = rpc.updateNfsTransferRange(rangeRequest);
    assertTrue(firstRange.getRecorded());
    String rangeKey = metadata.getNfsTransferRangeKey(exportId.toString(), writtenHandle.getVolumeId(),
        writtenHandle.getBucketId(), writtenHandle.getObjectId(), fileLease.getProtobuf().getGeneration(), 0,
        rangeRequest.getStagingKey());
    OmNfsTransferRangeInfo persistedRange = metadata.getNfsTransferRangeTable().get(rangeKey);
    assertNotNull(persistedRange, "Stable NFS range must be recorded by OM");
    assertEquals(data.length, persistedRange.getLength());
    assertEquals(ByteString.copyFrom(digest), persistedRange.getSha256());
    NfsTransferRangeResponse retryRange = rpc.updateNfsTransferRange(rangeRequest);
    assertTrue(retryRange.getRecorded(), "Range retries must be idempotent");
    assertEquals(firstRange.getTransactionIndex(), retryRange.getTransactionIndex());
    byte[] overlappingData = new byte[]{7, 8};
    String overlappingKey = exportId + "/" + transferId + "/overlap";
    try (OzoneOutputStream output = stagingBucket.createKey(overlappingKey, overlappingData.length, RATIS, ONE,
        new HashMap<>())) {
      output.write(overlappingData);
    }
    byte[] overlappingDigest = java.security.MessageDigest.getInstance("SHA-256").digest(overlappingData);
    NfsTransferRangeRequest overlappingRequest = rangeRequest.toBuilder().setOffset(1)
        .setLength(overlappingData.length).setStagingKey(volumeName + "/" + stagingBucketName + "/" + overlappingKey)
        .setSha256(ByteString.copyFrom(overlappingDigest)).build();
    NfsTransferRangeResponse overlappingRange = rpc.updateNfsTransferRange(overlappingRequest);
    assertTrue(overlappingRange.getRecorded(), "Later overlapping writes must be accepted");
    assertTrue(overlappingRange.getTransactionIndex() > firstRange.getTransactionIndex());
    byte[] shorterData = new byte[]{9};
    String shorterKey = exportId + "/" + transferId + "/shorter";
    try (OzoneOutputStream output = stagingBucket.createKey(shorterKey, shorterData.length, RATIS, ONE,
        new HashMap<>())) {
      output.write(shorterData);
    }
    byte[] shorterDigest = java.security.MessageDigest.getInstance("SHA-256").digest(shorterData);
    NfsTransferRangeRequest shorterRequest = rangeRequest.toBuilder().setLength(shorterData.length)
        .setStagingKey(volumeName + "/" + stagingBucketName + "/" + shorterKey)
        .setSha256(ByteString.copyFrom(shorterDigest)).build();
    NfsTransferRangeResponse shorterRange = rpc.updateNfsTransferRange(shorterRequest);
    assertTrue(shorterRange.getTransactionIndex() > overlappingRange.getTransactionIndex());
    LookupNfsTransferRangesRequest lookupRanges = LookupNfsTransferRangesRequest.newBuilder()
        .setVolumeName(volumeName).setBucketName(bucketName).setVolumeID(writtenHandle.getVolumeId())
        .setBucketID(writtenHandle.getBucketId()).setObjectID(writtenHandle.getObjectId())
        .setExportUUID(exportId.toString()).setGatewayID("nfs-write-it")
        .setGatewayGeneration(exportLease.getGatewayGeneration()).setTransferID(transferId)
        .setLeaseGeneration(fileLease.getProtobuf().getGeneration())
        .setOwnerToken(fileLease.getProtobuf().getOwnerToken()).setRequesterPrincipal(fileLease.getOwnerPrincipal())
        .setMaxEntries(1).build();
    LookupNfsTransferRangesResponse firstRangePage = rpc.lookupNfsTransferRanges(lookupRanges);
    assertEquals(1, firstRangePage.getRangesCount());
    assertFalse(firstRangePage.getEof());
    LookupNfsTransferRangesResponse secondRangePage = rpc.lookupNfsTransferRanges(
        lookupRanges.toBuilder().setStartAfterKey(firstRangePage.getNextMarker()).build());
    assertEquals(1, secondRangePage.getRangesCount());
    LookupNfsTransferRangesResponse lastRangePage = rpc.lookupNfsTransferRanges(
        lookupRanges.toBuilder().setStartAfterKey(secondRangePage.getNextMarker()).build());
    assertEquals(1, lastRangePage.getRangesCount());
    assertTrue(lastRangePage.getEof());
    assertNotNull(metadata.getNfsTransferRangeTable().get(rangeKey),
        "A shorter rewrite must preserve its earlier range for replaying the untouched tail");
    assertNfsRangePublication(bucket, exportId, key, exportLease, fileLease, writtenHandle,
        expectedNfsWriteData(data, overlappingData, shorterData));
    try (InputStream input = stagingBucket.readKey(stagingKey)) {
      assertArrayEquals(data, IOUtils.toByteArray(input));
    }
    NfsTransferRangeRequest deleteRange = rangeRequest.toBuilder()
        .setOperation(NfsTransferRangeRequest.Operation.DELETE).build();
    assertTrue(rpc.updateNfsTransferRange(deleteRange).getRecorded());
    assertNull(metadata.getNfsTransferRangeTable().get(rangeKey), "Committed ranges must be removable after assembly");
    String overlappingRangeKey = metadata.getNfsTransferRangeKey(exportId.toString(), writtenHandle.getVolumeId(),
        writtenHandle.getBucketId(), writtenHandle.getObjectId(), fileLease.getProtobuf().getGeneration(), 1,
        overlappingRequest.getStagingKey());
    assertNotNull(metadata.getNfsTransferRangeTable().get(overlappingRangeKey));
    assertTrue(rpc.updateNfsTransferRange(overlappingRequest.toBuilder()
        .setOperation(NfsTransferRangeRequest.Operation.DELETE).build()).getRecorded());
    assertNull(metadata.getNfsTransferRangeTable().get(overlappingRangeKey));
    String shorterRangeKey = metadata.getNfsTransferRangeKey(exportId.toString(), writtenHandle.getVolumeId(),
        writtenHandle.getBucketId(), writtenHandle.getObjectId(), fileLease.getProtobuf().getGeneration(), 0,
        shorterRequest.getStagingKey());
    assertTrue(rpc.updateNfsTransferRange(shorterRequest.toBuilder()
        .setOperation(NfsTransferRangeRequest.Operation.DELETE).build()).getRecorded());
    assertNull(metadata.getNfsTransferRangeTable().get(shorterRangeKey));
    stagingBucket.deleteKeys(Collections.singletonList(stagingKey));
    stagingBucket.deleteKeys(Collections.singletonList(overlappingKey));
    stagingBucket.deleteKeys(Collections.singletonList(shorterKey));
    try (InputStream input = bucket.readKey(key)) {
      assertArrayEquals(expectedNfsWriteData(data, overlappingData, shorterData), IOUtils.toByteArray(input));
    }
    assertNfsDirectoryMutationsBlocked(bucket, rpc, metadata, fs, key,
        expectedNfsWriteData(data, overlappingData, shorterData), renewFileLease);
  }

  private void assertNfsDirectoryMutationsBlocked(OzoneBucket bucket, OzoneManagerProtocol rpc,
      OMMetadataManager metadata, FileSystem fileSystem, String key, byte[] expectedData,
      NfsFileLeaseRequest fileLease) throws Exception {
    assertEquals(NFS_LEASE_CONFLICT, assertThrows(OMException.class, () -> {
      try (OzoneOutputStream output = bucket.createKey(key, expectedData.length, RATIS, ONE, new HashMap<>())) {
        output.write("overwrite".getBytes(StandardCharsets.UTF_8));
      }
    }).getResult());
    try (InputStream input = bucket.readKey(key)) {
      assertArrayEquals(expectedData, IOUtils.toByteArray(input), "A rejected overwrite must preserve NFS data");
    }
    assertEquals(NFS_LEASE_CONFLICT, assertThrows(OMException.class,
        () -> bucket.renameKey(key, "nfs/client-write-renamed")).getResult());
    OmDirectoryInfo nfsDirectory = getDirInfo("nfs");
    long volumeId = metadata.getVolumeId(volumeName);
    long bucketId = metadata.getBucketId(volumeName, bucketName);
    OmNfsObjectInfo leasedFile = metadata.getNfsObjectTable().get(
        metadata.getNfsObjectKey(volumeId, bucketId, fileLease.getObjectID()));
    assertNotNull(leasedFile);
    assertEquals(nfsDirectory.getObjectID(), leasedFile.getParentObjectID());
    OmNfsLeaseInfo activeLease = metadata.getNfsFileLeaseTable().get(
        metadata.getNfsFileLeaseKey(volumeId, bucketId, fileLease.getObjectID()));
    assertNotNull(activeLease);
    assertTrue(activeLease.getProtobuf().getExpiresAtMillis() > System.currentTimeMillis());
    assertEquals(NFS_LEASE_CONFLICT, assertThrows(OMException.class,
        () -> fileSystem.rename(new Path("/nfs"), new Path("/nfs-renamed"))).getResult());
    assertFalse(fileSystem.delete(new Path("/nfs"), true));
    assertTrue(fileSystem.exists(new Path("/nfs")), "Recursive delete must preserve an ancestor with a leased file");
    rpc.updateNfsFileLease(fileLease.toBuilder().setOperation(NfsFileLeaseRequest.Operation.RELEASE)
        .clearLeaseDurationMillis().build());
  }

  private void assertNfsRangePublication(OzoneBucket bucket, UUID exportId, String key,
      org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseResponse exportLease,
      OmNfsLeaseInfo fileLease, NfsFileHandle handle, byte[] publishedData)
      throws Exception {
    NfsFileCommitRequest commitRequest = NfsFileCommitRequest.newBuilder().setExportUUID(exportId.toString())
        .setGatewayID("nfs-write-it").setGatewayGeneration(exportLease.getGatewayGeneration())
        .setTransferID(fileLease.getProtobuf().getTransferID())
        .setFileGeneration(fileLease.getProtobuf().getGeneration())
        .setOwnerToken(fileLease.getProtobuf().getOwnerToken()).build();
    RpcClient rpcClient = (RpcClient) client.getObjectStore().getClientProxy();
    try (OzoneOutputStream output = rpcClient.openNfsFileWithLease(volumeName, bucketName, key,
        RatisReplicationConfig.getInstance(ONE.toProto()), commitRequest)) {
      output.write(publishedData);
    }
    assertEquals(handle.getObjectId(), nfsFileHandle(exportId, key).getObjectId(),
        "Publishing staged ranges must preserve the canonical file identity");
    try (InputStream input = bucket.readKey(key)) {
      assertArrayEquals(publishedData, IOUtils.toByteArray(input));
    }
  }

  private byte[] expectedNfsWriteData(byte[] data, byte[] overlappingData, byte[] shorterData) {
    byte[] expected = data.clone();
    expected[0] = shorterData[0];
    System.arraycopy(overlappingData, 0, expected, 1, overlappingData.length);
    return expected;
  }

  private void assertNfsObjectIndexEntries(OMMetadataManager metadata, NfsFileHandle handle, long directoryId,
      long parentId) throws IOException {
    assertEquals(new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, directoryId, "file"),
        metadata.getNfsObjectTable().get(metadata.getNfsObjectKey(handle.getVolumeId(), handle.getBucketId(),
            handle.getObjectId())));
    assertEquals(new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.DIRECTORY, parentId, "source"),
        metadata.getNfsObjectTable().get(metadata.getNfsObjectKey(handle.getVolumeId(), handle.getBucketId(),
            directoryId)));
    assertEquals(new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.DIRECTORY, handle.getBucketId(), "nfs"),
        metadata.getNfsObjectTable().get(metadata.getNfsObjectKey(handle.getVolumeId(), handle.getBucketId(),
            parentId)));
  }

  private void assertNfsExportLeaseRpc(OMMetadataManager metadata, OzoneManagerProtocol rpc, UUID exportId,
      String volume, String bucket) throws Exception {
    assertTrue(metadata.isNfsWriterLeaseEnabled());
    NfsExportLeaseRequest acquireRequest = NfsExportLeaseRequest.newBuilder().setOperation(Operation.ACQUIRE)
        .setExportUUID(exportId.toString()).setVolumeName(volume).setBucketName(bucket)
        .setGatewayID("nfs-gateway-it").setLeaseDurationMillis(30_000).build();
    org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseResponse acquired =
        rpc.updateNfsExportLease(acquireRequest);
    assertEquals(metadata.getVolumeId(volume), acquired.getVolumeID());
    assertEquals(metadata.getBucketTable().get(metadata.getBucketKey(volume, bucket)).getObjectID(),
        acquired.getBucketID());
    OMException activeLease = assertThrows(OMException.class, () -> rpc.updateNfsExportLease(acquireRequest));
    assertEquals(NFS_LEASE_CONFLICT, activeLease.getResult());
    NfsExportLeaseRequest renewRequest = acquireRequest.toBuilder().setOperation(Operation.RENEW)
        .setGatewayGeneration(acquired.getGatewayGeneration()).setGeneration(acquired.getGeneration()).build();
    org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.NfsExportLeaseResponse renewed =
        rpc.updateNfsExportLease(renewRequest);
    assertEquals(acquired.getGeneration(), renewed.getGeneration());
    assertEquals(acquired.getGatewayGeneration(), renewed.getGatewayGeneration());
    assertTrue(renewed.getExpiresAtMillis() > acquired.getExpiresAtMillis());
    rpc.updateNfsExportLease(renewRequest.toBuilder().setOperation(Operation.RELEASE).clearLeaseDurationMillis()
        .setGatewayGeneration(renewed.getGatewayGeneration()).setGeneration(renewed.getGeneration()).build());
    OmNfsLeaseInfo released = metadata.getNfsExportLeaseTable().get(metadata.getNfsExportLeaseKey(exportId.toString()));
    assertEquals(renewed.getGeneration() + 1, released.getProtobuf().getGeneration());
    assertTrue(released.getProtobuf().getExpiresAtMillis() < renewed.getExpiresAtMillis());
  }

  private void assertNfsDirectoryCursor(OzoneBucket bucket, OzoneManagerProtocol rpc, OmKeyArgs identityArgs,
      long volumeId, long bucketId) throws Exception {
    String cursorRoot = "nfs/cursor-" + UUID.randomUUID();
    String unicodeName = "café 文件";
    String unicodeFileRoot = "nfs/unicode-file-" + UUID.randomUUID();
    String unicodeDirectoryRoot = "nfs/unicode-directory-" + UUID.randomUUID();
    createTestKey(bucket, unicodeFileRoot + "/" + unicodeName, "unicode");
    NfsChildrenPage unicodeFile = rpc.lookupNfsChildren(identityArgs, volumeId, bucketId,
        getDirInfo(unicodeFileRoot).getObjectID(), NfsFileHandle.Kind.DIRECTORY, unicodeName, null, null, null);
    assertEquals(Collections.singletonList(unicodeName), unicodeFile.getChildren().stream()
        .map(status -> status.getKeyInfo().getFileName()).collect(Collectors.toList()));
    bucket.createDirectory(unicodeDirectoryRoot);
    bucket.createDirectory(unicodeDirectoryRoot + "/" + unicodeName);
    NfsChildrenPage unicodeDirectory = rpc.lookupNfsChildren(identityArgs, volumeId, bucketId,
        getDirInfo(unicodeDirectoryRoot).getObjectID(), NfsFileHandle.Kind.DIRECTORY, unicodeName,
        null, null, null);
    assertEquals(1, unicodeDirectory.getChildren().size());
    assertTrue(unicodeDirectory.getChildren().get(0).isDirectory());
    createTestKey(bucket, cursorRoot + "/a", "a");
    createTestKey(bucket, cursorRoot + "/b", "b");
    createTestKey(bucket, cursorRoot + "/c", "c");
    createTestKey(bucket, cursorRoot + "/unicode-directory/" + unicodeName, "unicode");
    long directoryId = getDirInfo(cursorRoot).getObjectID();
    long unicodeDirectoryId = getDirInfo(cursorRoot + "/unicode-directory").getObjectID();
    NfsChildrenPage unicodeChild = rpc.lookupNfsChildren(identityArgs, volumeId, bucketId, unicodeDirectoryId,
        NfsFileHandle.Kind.DIRECTORY, unicodeName, null, null, null);
    assertEquals(Collections.singletonList(unicodeName), unicodeChild.getChildren().stream()
        .map(status -> status.getKeyInfo().getFileName()).collect(Collectors.toList()));
    NfsChildrenPage firstPage = rpc.lookupNfsChildren(identityArgs, volumeId, bucketId, directoryId,
        NfsFileHandle.Kind.DIRECTORY, null, 2, null, null);
    assertEquals(Arrays.asList("a", "b"), firstPage.getChildren().stream()
        .map(status -> status.getKeyInfo().getFileName()).collect(Collectors.toList()));
    assertFalse(firstPage.isEof());
    long cursorObjectId = firstPage.getChildren().get(1).getKeyInfo().getObjectID();
    NfsChildrenPage secondPage = rpc.lookupNfsChildren(identityArgs, volumeId, bucketId, directoryId,
        NfsFileHandle.Kind.DIRECTORY, null, 2, cursorObjectId, firstPage.getVerifier());
    assertEquals(Arrays.asList("c", "unicode-directory"), secondPage.getChildren().stream()
        .map(status -> status.getKeyInfo().getFileName()).collect(Collectors.toList()));
    assertTrue(secondPage.isEof());
    createTestKey(bucket, cursorRoot + "/d", "d");
    assertEquals(OMException.ResultCodes.INVALID_REQUEST, assertThrows(OMException.class,
        () -> rpc.lookupNfsChildren(identityArgs, volumeId, bucketId, directoryId, NfsFileHandle.Kind.DIRECTORY,
            null, 2, cursorObjectId, firstPage.getVerifier())).getResult());
  }

  private void assertNfsReadRanges(NfsObjectReader gatewayReader, RequestContext callerContext,
      ByteString encodedHandle, OzoneBucket bucket, UUID exportId) throws IOException {
    NfsGatewayService gatewayService = new NfsGatewayService(gatewayReader);
    OmKeyInfo gatewayFile = gatewayReader.lookup(callerContext, encodedHandle, false).getKeyInfo();
    assertEquals(NfsFileHandle.fromBytes(encodedHandle.toByteArray()).getObjectId(), gatewayFile.getObjectID());
    assertTrue(gatewayFile.getLatestVersionLocations().getLocationListCount() > 0);
    assertTrue(gatewayReader.lookup(callerContext, encodedHandle, true).getKeyInfo()
        .getKeyLocationVersions().isEmpty());

    ReadResponse firstRange = readNfsThroughGateway(gatewayService, callerContext, encodedHandle, 0, 4);
    assertEquals(0, firstRange.getResult().getNfsStatus());
    assertEquals("NFS ", firstRange.getData().toStringUtf8());
    assertFalse(firstRange.getEof());
    ReadResponse finalRange = readNfsThroughGateway(gatewayService, callerContext, encodedHandle, 4,
        NfsObjectReader.MAX_READ_BYTES);
    assertEquals("identity contract", finalRange.getData().toStringUtf8());
    assertTrue(finalRange.getEof());
    assertTrue(readNfsThroughGateway(gatewayService, callerContext, encodedHandle, -1, 4).getEof());
    assertFalse(readNfsThroughGateway(gatewayService, callerContext, encodedHandle, 0, 0).getEof());
    createTestKey(bucket, "nfs/empty", "");
    ByteString emptyHandle = ByteString.copyFrom(nfsFileHandle(exportId, "nfs/empty").toBytes());
    ReadResponse empty = readNfsThroughGateway(gatewayService, callerContext, emptyHandle, 0, 4);
    assertTrue(empty.getData().isEmpty());
    assertTrue(empty.getEof());
    bucket.deleteKey("nfs/empty");
  }

  private ReadResponse readNfsThroughGateway(NfsGatewayService gatewayService, RequestContext callerContext,
      ByteString encodedHandle, long offset, int count) throws IOException {
    return invokeNfsGateway(observer -> gatewayService.read(ReadRequest.newBuilder().setContext(callerContext)
        .setHandle(encodedHandle).setOffset(offset).setCount(count).build(), observer));
  }

  private <T> T invokeNfsGateway(Consumer<StreamObserver<T>> dispatch) throws IOException {
    AtomicReference<T> response = new AtomicReference<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    StreamObserver<T> observer = new StreamObserver<T>() {
      @Override
      public void onNext(T value) {
        response.set(value);
      }

      @Override
      public void onError(Throwable error) {
        failure.set(error);
      }

      @Override
      public void onCompleted() {
      }
    };
    try (org.mockito.MockedStatic<NfsTlsPeerInterceptor> ignored = mockStatic(NfsTlsPeerInterceptor.class)) {
      dispatch.accept(observer);
    }
    if (failure.get() != null) {
      throw new IOException("NFS gateway request failed", failure.get());
    }
    assertNotNull(response.get(), "NFS gateway read must complete with a response");
    return response.get();
  }

  private void assertNfsNamespaceMutations(NfsObjectReader gatewayReader, RequestContext callerContext,
      OzoneBucket bucket, UUID exportId, long volumeId, long bucketId) throws Exception {
    NfsGatewayService gatewayService = new NfsGatewayService(gatewayReader);
    NfsFileHandle bucketHandle = new NfsFileHandle(exportId, volumeId, bucketId, bucketId,
        NfsFileHandle.Kind.BUCKET);
    ByteString encodedBucketHandle = ByteString.copyFrom(bucketHandle.toBytes());
    MkdirResponse createdDirectory = invokeNfsGateway(observer -> gatewayService.mkdir(MkdirRequest.newBuilder()
        .setContext(callerContext).setParentHandle(encodedBucketHandle).setName("nfs-mutation").build(), observer));
    assertEquals(NfsStatus.OK.getCode(), createdDirectory.getResult().getNfsStatus());
    assertTrue(createdDirectory.getAttributes().getDirectory());
    ByteString encodedDirectoryHandle = createdDirectory.getAttributes().getHandle();
    NfsFileHandle directoryHandle = NfsFileHandle.fromBytes(encodedDirectoryHandle.toByteArray());
    String unicodeName = "dir-café";
    MkdirResponse createdUnicodeDirectory = invokeNfsGateway(observer -> gatewayService.mkdir(MkdirRequest.newBuilder()
        .setContext(callerContext).setParentHandle(encodedBucketHandle).setName(unicodeName).build(), observer));
    assertEquals(NfsStatus.OK.getCode(), createdUnicodeDirectory.getResult().getNfsStatus());
    assertTrue(createdUnicodeDirectory.getAttributes().getDirectory());
    assertEquals(unicodeName, gatewayReader.lookupChildren(callerContext, encodedBucketHandle, unicodeName, null,
        null, null).getChildren().get(0).getKeyInfo().getFileName());
    MkdirResponse createdNested = invokeNfsGateway(observer -> gatewayService.mkdir(MkdirRequest.newBuilder()
        .setContext(callerContext).setParentHandle(encodedDirectoryHandle).setName("nested").build(), observer));
    assertEquals(NfsStatus.OK.getCode(), createdNested.getResult().getNfsStatus());
    RemoveResponse removedNested = invokeNfsGateway(observer -> gatewayService.remove(RemoveRequest.newBuilder()
        .setContext(callerContext).setParentHandle(encodedDirectoryHandle)
        .setObjectHandle(createdNested.getAttributes().getHandle()).setName("nested").build(), observer));
    assertEquals(NfsStatus.OK.getCode(), removedNested.getResult().getNfsStatus());
    createTestKey(bucket, "nfs-mutation/child", "child");
    ByteString childHandle = gatewayReader.lookupChildren(callerContext, encodedDirectoryHandle, "child", null,
        null, null).getChildren().stream().map(gatewayReader::handleFor).findFirst().get();

    RemoveResponse nonempty = invokeNfsGateway(observer -> gatewayService.remove(RemoveRequest.newBuilder()
        .setContext(callerContext).setParentHandle(encodedBucketHandle).setObjectHandle(encodedDirectoryHandle)
        .setName("nfs-mutation").build(), observer));
    assertEquals(NfsStatus.NOTEMPTY.getCode(), nonempty.getResult().getNfsStatus());

    RemoveResponse removedChild = invokeNfsGateway(observer -> gatewayService.remove(RemoveRequest.newBuilder()
        .setContext(callerContext).setParentHandle(encodedDirectoryHandle).setObjectHandle(childHandle)
        .setName("child").build(), observer));
    assertEquals(NfsStatus.OK.getCode(), removedChild.getResult().getNfsStatus());
    RenameResponse renamed = invokeNfsGateway(observer -> gatewayService.rename(RenameRequest.newBuilder()
        .setContext(callerContext).setObjectHandle(encodedDirectoryHandle).setOldParentHandle(encodedBucketHandle)
        .setOldName("nfs-mutation").setNewParentHandle(encodedBucketHandle).setNewName("nfs-renamed").build(),
        observer));
    assertEquals(NfsStatus.OK.getCode(), renamed.getResult().getNfsStatus());
    assertEquals(directoryHandle.getObjectId(), gatewayReader.lookup(callerContext,
        ByteString.copyFrom(directoryHandle.toBytes()), false).getKeyInfo().getObjectID());
    RemoveResponse removedDirectory = invokeNfsGateway(observer -> gatewayService.remove(RemoveRequest.newBuilder()
        .setContext(callerContext).setParentHandle(encodedBucketHandle).setObjectHandle(encodedDirectoryHandle)
        .setName("nfs-renamed").build(), observer));
    assertEquals(NfsStatus.OK.getCode(), removedDirectory.getResult().getNfsStatus());
    assertEquals(OMException.ResultCodes.FILE_NOT_FOUND, assertThrows(OMException.class,
        () -> gatewayReader.lookup(callerContext, ByteString.copyFrom(directoryHandle.toBytes()), false)).getResult());
  }

  private NfsFileHandle nfsFileHandle(UUID exportId, String key) throws IOException {
    OzoneManager om = cluster.getOzoneManager();
    OmKeyInfo info = om.lookupFile(new OmKeyArgs.Builder()
        .setVolumeName(volumeName).setBucketName(bucketName).setKeyName(key).build());
    OMMetadataManager metadata = om.getMetadataManager();
    return new NfsFileHandle(exportId, metadata.getVolumeId(volumeName), metadata.getBucketId(volumeName, bucketName),
        info.getObjectID(), NfsFileHandle.Kind.FILE);
  }

  @Test
  public void testRenameKey() throws IOException {
    String fromKeyName = UUID.randomUUID().toString();
    String value = "sample value";

    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume volume = objectStore.getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    createTestKey(bucket, fromKeyName, value);

    // Rename to an empty string means that we are moving the key to the bucket
    // level and the toKeyName will be the source key name
    String toKeyName = "";
    bucket.renameKey(fromKeyName, toKeyName);
    OzoneKey emptyKeyRename = bucket.getKey(fromKeyName);
    assertEquals(fromKeyName, emptyKeyRename.getName());

    toKeyName = UUID.randomUUID().toString();
    bucket.renameKey(fromKeyName, toKeyName);

    // Lookup for old key should fail.
    OMException e =
        assertThrows(OMException.class, () -> bucket.getKey(fromKeyName),
            "Lookup for old from key name should fail!");
    assertEquals(KEY_NOT_FOUND, e.getResult());
    OzoneKey key = bucket.getKey(toKeyName);
    assertEquals(toKeyName, key.getName());
  }

  @Test
  public void testKeyRenameWithSubDirs() throws Exception {
    String keyName1 = "dir1/dir2/file1";
    String keyName2 = "dir1/dir2/file2";

    String newKeyName1 = "dir1/key1";
    String newKeyName2 = "dir1/key2";

    String value = "sample value";
    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume volume = objectStore.getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    createTestKey(bucket, keyName1, value);
    createTestKey(bucket, keyName2, value);

    bucket.renameKey(keyName1, newKeyName1);
    bucket.renameKey(keyName2, newKeyName2);

    // new key should exist
    assertEquals(newKeyName1, bucket.getKey(newKeyName1).getName());
    assertEquals(newKeyName2, bucket.getKey(newKeyName2).getName());

    // old key should not exist
    assertKeyRenamedEx(bucket, keyName1);
    assertKeyRenamedEx(bucket, keyName2);
  }

  @Test
  public void testRenameToAnExistingKey() throws Exception {
    String keyName1 = "dir1/dir2/file1";
    String keyName2 = "dir1/dir2/file2";

    String value = "sample value";
    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume volume = objectStore.getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    createTestKey(bucket, keyName1, value);
    createTestKey(bucket, keyName2, value);
    OMException e =
        assertThrows(OMException.class, () -> bucket.renameKey(keyName1, keyName2),
            "Should throw exception as destin key already exists!");
    assertEquals(KEY_ALREADY_EXISTS, e.getResult());
  }

  @Test
  public void testCreateBucketWithBucketLayout() throws Exception {
    String sampleVolumeName = UUID.randomUUID().toString();
    String sampleBucketName = UUID.randomUUID().toString();
    ObjectStore store = client.getObjectStore();
    store.createVolume(sampleVolumeName);
    OzoneVolume volume = store.getVolume(sampleVolumeName);

    // Case 1: Bucket layout: FILE_SYSTEM_OPTIMIZED
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    volume.createBucket(sampleBucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(sampleBucketName);
    assertEquals(sampleBucketName, bucket.getName());
    assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED,
        bucket.getBucketLayout());

    // Case 2: Bucket layout: OBJECT_STORE
    sampleBucketName = UUID.randomUUID().toString();
    builder.setBucketLayout(BucketLayout.OBJECT_STORE);
    volume.createBucket(sampleBucketName, builder.build());
    bucket = volume.getBucket(sampleBucketName);
    assertEquals(sampleBucketName, bucket.getName());
    assertEquals(BucketLayout.OBJECT_STORE, bucket.getBucketLayout());

    // Case 3: Bucket layout: Empty and
    // OM default bucket layout: FILE_SYSTEM_OPTIMIZED
    builder = BucketArgs.newBuilder();
    sampleBucketName = UUID.randomUUID().toString();
    volume.createBucket(sampleBucketName, builder.build());
    bucket = volume.getBucket(sampleBucketName);
    assertEquals(sampleBucketName, bucket.getName());
    assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED,
        bucket.getBucketLayout());

    // Case 4: Bucket layout: Empty
    sampleBucketName = UUID.randomUUID().toString();
    builder = BucketArgs.newBuilder();
    volume.createBucket(sampleBucketName, builder.build());
    bucket = volume.getBucket(sampleBucketName);
    assertEquals(sampleBucketName, bucket.getName());
    assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED,
        bucket.getBucketLayout());

    // Case 5: Bucket layout: LEGACY
    sampleBucketName = UUID.randomUUID().toString();
    builder.setBucketLayout(BucketLayout.LEGACY);
    volume.createBucket(sampleBucketName, builder.build());
    bucket = volume.getBucket(sampleBucketName);
    assertEquals(sampleBucketName, bucket.getName());
    assertEquals(BucketLayout.LEGACY, bucket.getBucketLayout());
  }

  private void assertKeyRenamedEx(OzoneBucket bucket, String keyName)
      throws Exception {
    OMException ome =
        assertThrows(OMException.class, () -> bucket.getKey(keyName),
            "Should throw KeyNotFound as the key got renamed!");
    assertEquals(KEY_NOT_FOUND, ome.getResult());
  }

  private void createTestKey(OzoneBucket bucket, String keyName,
      String keyValue) throws IOException {
    OzoneOutputStream out = bucket.createKey(keyName,
            keyValue.getBytes(StandardCharsets.UTF_8).length, RATIS,
            ONE, new HashMap<>());
    out.write(keyValue.getBytes(StandardCharsets.UTF_8));
    out.close();
    OzoneKey key = bucket.getKey(keyName);
    assertEquals(keyName, key.getName());
  }

  private OmDirectoryInfo getDirInfo(String parentKey) throws Exception {
    OMMetadataManager omMetadataManager =
            cluster.getOzoneManager().getMetadataManager();
    long volumeId = omMetadataManager.getVolumeId(volumeName);
    long bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
    String[] pathComponents = StringUtils.split(parentKey, '/');
    long parentId = bucketId;
    OmDirectoryInfo dirInfo = null;

    for (String pathElement : pathComponents) {
      String dbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId, parentId, pathElement);
      dirInfo = omMetadataManager.getDirectoryTable().get(dbKey);
      parentId = dirInfo.getObjectID();
    }
    return dirInfo;
  }

  private void verifyKeyInFileTable(Table<String, OmKeyInfo> fileTable,
      String fileName, long parentID, boolean isEmpty) throws IOException {

    final OMMetadataManager omMetadataManager =
            cluster.getOzoneManager().getMetadataManager();
    final String dbFileKey = omMetadataManager.getOzonePathKey(
            omMetadataManager.getVolumeId(volumeName),
            omMetadataManager.getBucketId(volumeName, bucketName),
            parentID, fileName);
    OmKeyInfo omKeyInfo = fileTable.get(dbFileKey);
    if (isEmpty) {
      assertNull(omKeyInfo, "Table is not empty!");
    } else {
      assertNotNull(omKeyInfo, "Table is empty!");
      // used startsWith because the key format is,
      // <parentID>/fileName/<clientID> and clientID is not visible.
      assertEquals(omKeyInfo.getKeyName(), fileName,
          "Invalid Key: " + omKeyInfo.getObjectInfo());
      assertEquals(parentID, omKeyInfo.getParentObjectID(), "Invalid Key");
    }
  }

  private void verifyKeyInOpenFileTable(Table<String, OmKeyInfo> openFileTable,
      long clientID, String fileName, long parentID, boolean isEmpty)
          throws IOException, TimeoutException, InterruptedException {
    final OMMetadataManager omMetadataManager =
            cluster.getOzoneManager().getMetadataManager();
    final String dbOpenFileKey = omMetadataManager.getOpenFileName(
            omMetadataManager.getVolumeId(volumeName),
            omMetadataManager.getBucketId(volumeName, bucketName),
            parentID, fileName, clientID);

    if (isEmpty) {
      // wait for DB updates
      GenericTestUtils.waitFor(() -> {
        try {
          OmKeyInfo omKeyInfo = openFileTable.get(dbOpenFileKey);
          return omKeyInfo == null;
        } catch (IOException e) {
          fail("DB failure!");
          return false;
        }

      }, 1000, 120000);
    } else {
      OmKeyInfo omKeyInfo = openFileTable.get(dbOpenFileKey);
      assertNotNull(omKeyInfo, "Table is empty!");
      assertEquals(omKeyInfo.getFileName(), fileName, "Invalid file name: " + omKeyInfo.getObjectInfo());
      assertEquals(parentID, omKeyInfo.getParentObjectID(), "Invalid Key");
    }
  }

  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  @AfterAll
  void cleanup() {
    IOUtils.closeQuietly(client);
  }
}
