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

package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.ozone.OzoneFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.snapshot.TestOmSnapshot;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.fs.ozone.Constants.LISTING_PAGE_SIZE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPath;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.LEGACY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Abstract class for OmSnapshot file system tests.
 */
@Timeout(120)
public abstract class TestOmSnapshotFileSystem {
  protected static final String VOLUME_NAME =
      "volume" + RandomStringUtils.randomNumeric(5);
  protected static final String BUCKET_NAME_FSO =
      "bucket-fso-" + RandomStringUtils.randomNumeric(5);
  protected static final String BUCKET_NAME_LEGACY =
      "bucket-legacy-" + RandomStringUtils.randomNumeric(5);

  private static MiniOzoneCluster cluster = null;
  private static OzoneClient client;
  private static ObjectStore objectStore;
  private static OzoneConfiguration conf;
  private static OzoneManagerProtocol writeClient;
  private static OzoneManager ozoneManager;
  private static String keyPrefix;
  private final String bucketName;
  private FileSystem fs;
  private OzoneFileSystem o3fs;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOmSnapshot.class);

  public TestOmSnapshotFileSystem(String bucketName) {
    this.bucketName = bucketName;
  }

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    objectStore = client.getObjectStore();
    writeClient = objectStore.getClientProxy().getOzoneManagerClient();
    ozoneManager = cluster.getOzoneManager();

    TestDataUtil.createVolume(client, VOLUME_NAME);
    TestDataUtil.createBucket(client, VOLUME_NAME,
        new BucketArgs.Builder().setBucketLayout(FILE_SYSTEM_OPTIMIZED).build(),
        BUCKET_NAME_FSO);
    TestDataUtil.createBucket(client, VOLUME_NAME,
        new BucketArgs.Builder().setBucketLayout(LEGACY).build(),
        BUCKET_NAME_LEGACY);

    // stop the deletion services so that keys can still be read
    KeyManagerImpl keyManager = (KeyManagerImpl) ozoneManager.getKeyManager();
    keyManager.stop();
  }

  @BeforeEach
  public void setupFsClient() throws IOException {
    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucketName, VOLUME_NAME);
    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);
    fs = FileSystem.get(conf);
    o3fs = (OzoneFileSystem) fs;
  }

  @AfterAll
  public static void tearDown() throws Exception {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Cleanup files and directories.
   *
   * @throws IOException DB failure
   */
  @AfterEach
  public void deleteRootDir()
      throws IOException, InterruptedException, TimeoutException {
    Path root = new Path("/");
    FileStatus[] fileStatuses = fs.listStatus(root);

    if (fileStatuses == null) {
      return;
    }

    for (FileStatus fStatus : fileStatuses) {
      fs.delete(fStatus.getPath(), true);
    }


    GenericTestUtils.waitFor(() -> {
      try {
        return fs.listStatus(root).length == 0;
      } catch (Exception e) {
        return false;
      }
    }, 1000, 120000);

    IOUtils.closeQuietly(fs);
    IOUtils.closeQuietly(o3fs);
  }

  @Test
  // based on TestObjectStoreWithFSO:testListKeysAtDifferentLevels
  public void testListKeysAtDifferentLevels() throws Exception {
    OzoneVolume ozoneVolume = objectStore.getVolume(VOLUME_NAME);
    assertEquals(ozoneVolume.getName(), VOLUME_NAME);
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    assertEquals(ozoneBucket.getName(), bucketName);

    List<String> keys = Arrays.asList(
        "/a/b1/c1/c1.tx",
        "/a/b1/c2/c2.tx",
        "/a/b2/d1/d11.tx",
        "/a/b2/d2/d21.tx",
        "/a/b2/d2/d22.tx",
        "/a/b2/d3/d31.tx",
        "/a/b3/e1/e11.tx",
        "/a/b3/e2/e21.tx",
        "/a/b3/e3/e31.tx");

    createKeys(ozoneBucket, keys);
    String snapshotName = UUID.randomUUID().toString();
    setKeyPrefix(createSnapshot(snapshotName).substring(1));

    // Delete the active fs so that we don't inadvertently read it
    deleteRootDir();
    // Root level listing keys
    Iterator<? extends OzoneKey> ozoneKeyIterator =
        ozoneBucket.listKeys(keyPrefix, null);
    verifyFullTreeStructure(ozoneKeyIterator);

    ozoneKeyIterator = ozoneBucket.listKeys(keyPrefix + "a/", null);
    verifyFullTreeStructure(ozoneKeyIterator);

    LinkedList<String> expectedKeys;

    // Intermediate level keyPrefix - 2nd level
    ozoneKeyIterator = ozoneBucket.listKeys(keyPrefix + "a/b2/", null);
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
    ozoneKeyIterator = ozoneBucket.listKeys(keyPrefix + "a/b2/d1", null);
    expectedKeys = new LinkedList<>();
    expectedKeys.add("a/b2/d1/");
    expectedKeys.add("a/b2/d1/d11.tx");
    checkKeyList(ozoneKeyIterator, expectedKeys);

    // Boundary of a level
    ozoneKeyIterator = ozoneBucket.listKeys(keyPrefix + "a/b2/d2",
        keyPrefix + "a/b2/d2/d21.tx");
    expectedKeys = new LinkedList<>();
    expectedKeys.add("a/b2/d2/d22.tx");
    checkKeyList(ozoneKeyIterator, expectedKeys);

    // Boundary case - last node in the depth-first-traversal
    ozoneKeyIterator = ozoneBucket.listKeys(keyPrefix + "a/b3/e3",
        keyPrefix + "a/b3/e3/e31.tx");
    expectedKeys = new LinkedList<>();
    checkKeyList(ozoneKeyIterator, expectedKeys);

    deleteSnapshot(snapshotName);
    String expectedMessage = String.format("Unable to load snapshot. " +
            "Snapshot with table key '/%s/%s/%s' is no longer active",
        VOLUME_NAME, bucketName, snapshotName);
    OMException exception = assertThrows(OMException.class,
        () -> ozoneBucket.listKeys(keyPrefix + "a/", null));
    assertEquals(expectedMessage, exception.getMessage());
    exception = assertThrows(OMException.class,
        () -> ozoneBucket.listKeys(keyPrefix + "a/b2/", null));
    assertEquals(expectedMessage, exception.getMessage());
    exception = assertThrows(OMException.class,
        () -> ozoneBucket.listKeys(keyPrefix + "a/b2/d1", null));
    assertEquals(expectedMessage, exception.getMessage());
    exception = assertThrows(OMException.class,
        () -> ozoneBucket.listKeys(keyPrefix + "a/b2/d2",
            keyPrefix + "a/b2/d2/d21.tx"));
    assertEquals(expectedMessage, exception.getMessage());
    exception = assertThrows(OMException.class,
        () -> ozoneBucket.listKeys(keyPrefix + "a/b3/e3",
            keyPrefix + "a/b3/e3/e31.tx"));
    assertEquals(expectedMessage, exception.getMessage());
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

  private void checkKeyList(Iterator<? extends OzoneKey> ozoneKeyIterator,
                            List<String> keys) {

    LinkedList<String> outputKeys = new LinkedList<>();
    while (ozoneKeyIterator.hasNext()) {
      OzoneKey ozoneKey = ozoneKeyIterator.next();
      String keyName = ozoneKey.getName();
      if (keyName.startsWith(keyPrefix)) {
        keyName = keyName.substring(keyPrefix.length());
      }
      outputKeys.add(keyName);
    }
    assertEquals(keys, outputKeys);
  }

  private void createKeys(OzoneBucket ozoneBucket, List<String> keys)
      throws Exception {
    int length = 10;
    byte[] input = new byte[length];
    Arrays.fill(input, (byte) 96);
    for (String key : keys) {
      createKey(ozoneBucket, key, 10, input);
    }
  }

  private void createKey(OzoneBucket ozoneBucket, String key, int length,
                         byte[] input) throws Exception {

    OzoneOutputStream ozoneOutputStream =
        ozoneBucket.createKey(key, length);

    ozoneOutputStream.write(input);
    ozoneOutputStream.write(input, 0, 10);
    ozoneOutputStream.close();

    // Read the key with given key name.
    OzoneInputStream ozoneInputStream = ozoneBucket.readKey(key);
    byte[] read = new byte[length];
    ozoneInputStream.read(read, 0, length);
    ozoneInputStream.close();

    String inputString = new String(input, StandardCharsets.UTF_8);
    assertEquals(inputString, new String(read, StandardCharsets.UTF_8));

    // Read using filesystem.
    String rootPath = String.format("%s://%s.%s/", OZONE_URI_SCHEME,
        bucketName, VOLUME_NAME);
    OzoneFileSystem o3fsNew = (OzoneFileSystem) FileSystem
        .get(new URI(rootPath), conf);
    FSDataInputStream fsDataInputStream = o3fsNew.open(new Path(key));
    read = new byte[length];
    fsDataInputStream.read(read, 0, length);
    fsDataInputStream.close();

    assertEquals(inputString, new String(read, StandardCharsets.UTF_8));
  }

  private static void setKeyPrefix(String s) {
    keyPrefix = s;
  }

  @Test
  public void testBlockSnapshotFSAccessAfterDeletion() throws Exception {
    Path root = new Path("/");
    Path dir = new Path(root, "/testListKeysBeforeAfterSnapshotDeletion");
    Path key1 = new Path(dir, "key1");
    Path key2 = new Path(dir, "key2");

    // Create 2 keys
    ContractTestUtils.touch(fs, key1);
    ContractTestUtils.touch(fs, key2);

    // Create a snapshot
    String snapshotName = UUID.randomUUID().toString();
    String snapshotKeyPrefix = createSnapshot(snapshotName);

    // Can list keys in snapshot
    Path snapshotRoot = new Path(snapshotKeyPrefix + root);
    Path snapshotParent = new Path(snapshotKeyPrefix + dir);
    // Check dir in snapshot
    FileStatus[] fileStatuses = o3fs.listStatus(snapshotRoot);
    assertEquals(1, fileStatuses.length);
    // List keys in dir in snapshot
    fileStatuses = o3fs.listStatus(snapshotParent);
    assertEquals(2, fileStatuses.length);

    // Check key metadata
    Path snapshotKey1 = new Path(snapshotKeyPrefix + key1);
    FileStatus fsActiveKey = o3fs.getFileStatus(key1);
    FileStatus fsSnapshotKey = o3fs.getFileStatus(snapshotKey1);
    assertEquals(fsActiveKey.getModificationTime(),
        fsSnapshotKey.getModificationTime());

    Path snapshotKey2 = new Path(snapshotKeyPrefix + key2);
    fsActiveKey = o3fs.getFileStatus(key2);
    fsSnapshotKey = o3fs.getFileStatus(snapshotKey2);
    assertEquals(fsActiveKey.getModificationTime(),
        fsSnapshotKey.getModificationTime());

    // Delete the snapshot
    deleteSnapshot(snapshotName);

    // Can't access keys in snapshot anymore with FS API. Should throw exception
    final String errorMsg1 = "no longer active";
    FileNotFoundException exception = assertThrows(FileNotFoundException.class,
        () -> o3fs.listStatus(snapshotRoot));
    assertThat(exception.getMessage()).contains(errorMsg1);
    exception = assertThrows(FileNotFoundException.class,
        () -> o3fs.listStatus(snapshotParent));
    assertThat(exception.getMessage()).contains(errorMsg1);

    // Note: Different error message due to inconsistent FNFE client-side
    //  handling in BasicOzoneClientAdapterImpl#getFileStatus
    // TODO: Reconciliation?
    final String errorMsg2 = "No such file or directory";
    exception = assertThrows(FileNotFoundException.class,
        () -> o3fs.getFileStatus(snapshotKey1));
    assertThat(exception.getMessage()).contains(errorMsg2);
    exception = assertThrows(FileNotFoundException.class,
        () -> o3fs.getFileStatus(snapshotKey2));
    assertThat(exception.getMessage()).contains(errorMsg2);
  }

  @Test
  // based on TestOzoneFileSystem:testListStatus
  public void testListStatus() throws Exception {
    Path root = new Path("/");
    Path parent = new Path(root, "/testListStatus");
    Path file1 = new Path(parent, "key1");
    Path file2 = new Path(parent, "key2");

    String snapshotName1 = UUID.randomUUID().toString();
    String snapshotKeyPrefix1 = createSnapshot(snapshotName1);
    Path snapshotRoot1 = new Path(snapshotKeyPrefix1 + root);
    FileStatus[] fileStatuses = o3fs.listStatus(snapshotRoot1);
    assertEquals(0, fileStatuses.length, "Should be empty");

    ContractTestUtils.touch(fs, file1);
    ContractTestUtils.touch(fs, file2);

    String snapshotName2 = UUID.randomUUID().toString();
    String snapshotKeyPrefix2 = createSnapshot(snapshotName2);
    Path snapshotRoot2 = new Path(snapshotKeyPrefix2 + root);
    Path snapshotParent2 = new Path(snapshotKeyPrefix2 + parent);
    fileStatuses = o3fs.listStatus(snapshotRoot2);
    assertEquals(1, fileStatuses.length,
        "Should have created parent");
    assertEquals(fileStatuses[0].getPath().toUri().getPath(),
        snapshotParent2.toString(), "Parent path doesn't match");

    // ListStatus on a directory should return all subdirs along with
    // files, even if there exists a file and sub-dir with the same name.
    fileStatuses = o3fs.listStatus(snapshotParent2);
    assertEquals(2, fileStatuses.length,
        "FileStatus did not return all children of the directory");
    // ListStatus should return only the immediate children of a directory.
    Path file3 = new Path(parent, "dir1/key3");
    Path file4 = new Path(parent, "dir1/key4");
    ContractTestUtils.touch(fs, file3);
    ContractTestUtils.touch(fs, file4);

    String snapshotName3 = UUID.randomUUID().toString();
    String snapshotKeyPrefix3 = createSnapshot(snapshotName3);
    Path snapshotParent3 = new Path(snapshotKeyPrefix3 + parent);
    deleteRootDir();
    fileStatuses = o3fs.listStatus(snapshotParent3);
    assertEquals(3, fileStatuses.length,
        "FileStatus did not return all children of the directory");

    deleteSnapshot(snapshotName1);
    FileNotFoundException exception1 = assertThrows(FileNotFoundException.class,
        () -> fs.listStatus(snapshotRoot1));
    assertEquals(String.format("Unable to load snapshot. " +
            "Snapshot with table key '/%s/%s/%s' is no longer active",
        VOLUME_NAME, bucketName, snapshotName1), exception1.getMessage());

    deleteSnapshot(snapshotName2);
    FileNotFoundException exception2 = assertThrows(FileNotFoundException.class,
        () -> fs.listStatus(snapshotRoot2));
    assertEquals(String.format("Unable to load snapshot. " +
            "Snapshot with table key '/%s/%s/%s' is no longer active",
        VOLUME_NAME, bucketName, snapshotName2), exception2.getMessage());

    deleteSnapshot(snapshotName3);
    FileNotFoundException exception3 = assertThrows(FileNotFoundException.class,
        () -> fs.listStatus(snapshotParent3));
    assertEquals(String.format("Unable to load snapshot. " +
            "Snapshot with table key '/%s/%s/%s' is no longer active",
        VOLUME_NAME, bucketName, snapshotName3), exception3.getMessage());
  }

  @Test
  // based on TestOzoneFileSystem:testListStatusWithIntermediateDir
  public void testListStatusWithIntermediateDir() throws Exception {
    String keyName = "object-dir/object-name";
    createAndCommitKey(keyName);

    Path parent = new Path("/");

    GenericTestUtils.waitFor(() -> {
      try {
        return fs.listStatus(parent).length != 0;
      } catch (IOException e) {
        LOG.error("listStatus() Failed", e);
        fail("listStatus() Failed");
        return false;
      }
    }, 1000, 120000);

    String snapshotName = UUID.randomUUID().toString();
    String snapshotKeyPrefix = createSnapshot(snapshotName);
    deleteRootDir();
    Path snapshotParent = new Path(snapshotKeyPrefix + parent);
    FileStatus[] fileStatuses = fs.listStatus(snapshotParent);

    // the number of immediate children of root is 1
    assertEquals(1, fileStatuses.length);

    deleteSnapshot(snapshotName);
    FileNotFoundException exception = assertThrows(FileNotFoundException.class,
        () -> fs.listStatus(snapshotParent));
    assertEquals(String.format("Unable to load snapshot. " +
            "Snapshot with table key '/%s/%s/%s' is no longer active",
        VOLUME_NAME, bucketName, snapshotName), exception.getMessage());
  }

  @Test
  public void testGetFileStatus() throws Exception {
    String dir = "dir";
    String keyName = dir + "/" + "key";
    createAndCommitKey(keyName);

    Path parent = new Path("/");

    GenericTestUtils.waitFor(() -> {
      try {
        return fs.listStatus(parent).length != 0;
      } catch (IOException e) {
        LOG.error("listStatus() Failed", e);
        fail("listStatus() Failed");
        return false;
      }
    }, 1000, 120000);

    String snapshotName = UUID.randomUUID().toString();
    String snapshotKeyPrefix = createSnapshot(snapshotName);
    Path snapshotParent = new Path(snapshotKeyPrefix + parent);
    Path dirInSnapshot = new Path(snapshotKeyPrefix + parent + dir);
    Path keyInSnapshot = new Path(snapshotKeyPrefix + parent + keyName);

    assertEquals(1, fs.listStatus(snapshotParent).length);
    assertFalse(fs.getFileStatus(dirInSnapshot).isFile());
    assertTrue(fs.getFileStatus(keyInSnapshot).isFile());

    deleteSnapshot(snapshotName);
    FileNotFoundException exception = assertThrows(FileNotFoundException.class,
        () -> fs.listStatus(snapshotParent));
    assertEquals(String.format("Unable to load snapshot. " +
            "Snapshot with table key '/%s/%s/%s' is no longer active",
        VOLUME_NAME, bucketName, snapshotName), exception.getMessage());
  }

  @Test
  public void testReadFileFromSnapshot() throws Exception {
    String keyName = "dir/file";
    byte[] strBytes = "Sample text".getBytes(StandardCharsets.UTF_8);
    Path parent = new Path("/");
    Path file = new Path(parent, "dir/file");
    try (FSDataOutputStream out1 = fs.create(file, FsPermission.getDefault(),
        true, 8, (short) 3, 1, null)) {
      out1.write(strBytes);
    }

    GenericTestUtils.waitFor(() -> {
      try {
        return fs.listStatus(parent).length != 0;
      } catch (IOException e) {
        LOG.error("listStatus() Failed", e);
        fail("listStatus() Failed");
        return false;
      }
    }, 1000, 120000);

    String snapshotName = UUID.randomUUID().toString();
    String snapshotKeyPrefix = createSnapshot(snapshotName);
    Path fileInSnapshot = new Path(snapshotKeyPrefix + parent + keyName);

    try (FSDataInputStream inputStream = fs.open(fileInSnapshot)) {
      ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
      inputStream.read(buffer);
      byte[] readBytes = new byte[strBytes.length];
      System.arraycopy(buffer.array(), 0, readBytes, 0, strBytes.length);
      assertArrayEquals(strBytes, readBytes);
    } catch (Exception e) {
      fail("Failed to read file, Exception : " + e);
    }

    deleteSnapshot(snapshotName);
    FileNotFoundException exception = assertThrows(FileNotFoundException.class,
        () -> fs.open(fileInSnapshot));
    assertEquals(String.format("FILE_NOT_FOUND: Unable to load snapshot. " +
            "Snapshot with table key '/%s/%s/%s' is no longer active",
        VOLUME_NAME, bucketName, snapshotName), exception.getMessage());
  }

  private void createAndCommitKey(String keyName) throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(VOLUME_NAME)
        .setBucketName(bucketName).setKeyName(keyName)
        .setAcls(Collections.emptyList())
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
        .setLocationInfoList(new ArrayList<>()).build();

    OpenKeySession session = writeClient.openKey(keyArgs);
    writeClient.commitKey(keyArgs, session.getId());
  }

  /**
   * Tests listStatus operation on root directory.
   */
  @Test
  // based on TestOzoneFileSystem:testListStatusOnRoot
  public void testListStatusOnRoot() throws Exception {
    Path root = new Path("/");
    Path dir1 = new Path(root, "dir1");
    Path dir12 = new Path(dir1, "dir12");
    Path dir2 = new Path(root, "dir2");
    fs.mkdirs(dir12);
    fs.mkdirs(dir2);

    // ListStatus on root should return dir1 (even though /dir1 key does not
    // exist) and dir2 only. dir12 is not an immediate child of root and
    // hence should not be listed.
    String snapshotName = UUID.randomUUID().toString();
    String snapshotKeyPrefix = createSnapshot(snapshotName);
    deleteRootDir();
    Path snapshotRoot = new Path(snapshotKeyPrefix + root);
    FileStatus[] fileStatuses = o3fs.listStatus(snapshotRoot);
    assertEquals(2, fileStatuses.length,
        "FileStatus should return only the immediate children");

    // Verify that dir12 is not included in the result of the listStatus on root
    String fileStatus1 = fileStatuses[0].getPath().toUri().getPath();
    String fileStatus2 = fileStatuses[1].getPath().toUri().getPath();
    assertNotEquals(fileStatus1, dir12.toString());
    assertNotEquals(fileStatus2, dir12.toString());

    deleteSnapshot(snapshotName);
    FileNotFoundException exception = assertThrows(FileNotFoundException.class,
        () -> fs.listStatus(snapshotRoot));
    assertEquals(String.format("Unable to load snapshot. " +
            "Snapshot with table key '/%s/%s/%s' is no longer active",
        VOLUME_NAME, bucketName, snapshotName), exception.getMessage());
  }

  /**
   * Tests listStatus operation on root directory.
   */
  @Test
  // based on TestOzoneFileSystem:testListStatusOnLargeDirectory
  public void testListStatusOnLargeDirectory() throws Exception {
    Path root = new Path("/");
    Set<String> paths = new TreeSet<>();
    int numDirs = LISTING_PAGE_SIZE + LISTING_PAGE_SIZE / 2;
    for (int i = 0; i < numDirs; i++) {
      Path p = new Path(root, String.valueOf(i));
      fs.mkdirs(p);
      paths.add(p.getName());
    }

    String snapshotName = UUID.randomUUID().toString();
    String snapshotKeyPrefix = createSnapshot(snapshotName);
    deleteRootDir();
    Path snapshotRoot = new Path(snapshotKeyPrefix + root);
    FileStatus[] fileStatuses = o3fs.listStatus(snapshotRoot);
    // Added logs for debugging failures, to check any sub-path mismatches.
    Set<String> actualPaths = new TreeSet<>();
    ArrayList<String> actualPathList = new ArrayList<>();
    if (numDirs != fileStatuses.length) {
      for (int i = 0; i < fileStatuses.length; i++) {
        boolean duplicate =
            actualPaths.add(fileStatuses[i].getPath().getName());
        if (!duplicate) {
          LOG.info("Duplicate path:{} in FileStatusList",
              fileStatuses[i].getPath().getName());
        }
        actualPathList.add(fileStatuses[i].getPath().getName());
      }
      if (numDirs != actualPathList.size()) {
        LOG.info("actualPathsSize: {}", actualPaths.size());
        LOG.info("actualPathListSize: {}", actualPathList.size());
        actualPaths.removeAll(paths);
        actualPathList.removeAll(paths);
        LOG.info("actualPaths: {}", actualPaths);
        LOG.info("actualPathList: {}", actualPathList);
      }
    }
    assertEquals(numDirs, fileStatuses.length,
        "Total directories listed do not match the existing directories");

    for (int i = 0; i < numDirs; i++) {
      assertThat(paths).contains(fileStatuses[i].getPath().getName());
    }

    deleteSnapshot(snapshotName);
    FileNotFoundException exception = assertThrows(FileNotFoundException.class,
        () -> fs.listStatus(snapshotRoot));
    assertEquals(String.format("Unable to load snapshot. " +
            "Snapshot with table key '/%s/%s/%s' is no longer active",
        VOLUME_NAME, bucketName, snapshotName), exception.getMessage());
  }

  private String createSnapshot(String snapshotName)
      throws IOException, InterruptedException, TimeoutException {

    // create snapshot
    writeClient.createSnapshot(VOLUME_NAME, bucketName, snapshotName);

    // wait till the snapshot directory exists
    SnapshotInfo snapshotInfo = ozoneManager.getMetadataManager()
        .getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(VOLUME_NAME, bucketName, snapshotName));
    String snapshotDirName = getSnapshotPath(conf, snapshotInfo) +
        OM_KEY_PREFIX + "CURRENT";
    GenericTestUtils.waitFor(() -> new File(snapshotDirName).exists(),
        1000, 120000);

    return OM_KEY_PREFIX + OmSnapshotManager.getSnapshotPrefix(snapshotName);
  }

  private void deleteSnapshot(String snapshotName) throws IOException {
    writeClient.deleteSnapshot(VOLUME_NAME, bucketName, snapshotName);
  }
}
