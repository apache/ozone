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
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ozone.OzoneFileSystem;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

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
    OzoneBucket bucket = TestDataUtil
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
    String file = "key" + RandomStringUtils.secure().nextNumeric(5);
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
    OzoneBucket testBucket = TestDataUtil
        .createVolumeAndBucket(client, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    String testVolumeName = testBucket.getVolumeName();
    String testBucketName = testBucket.getName();

    String parent = "a/b/c/";
    String file = "key" + RandomStringUtils.secure().nextNumeric(5);
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
    String fileName = "key" + RandomStringUtils.secure().nextNumeric(5);
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
      byte[] input = TestDataUtil.createStringKey(ozoneBucket, key, 10);
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
        bucketName, volumeName, StandardCharsets.UTF_8);
    OzoneFileSystem o3fs = (OzoneFileSystem) FileSystem.get(new URI(rootPath),
        conf);
    try (InputStream fsDataInputStream = o3fs.open(new Path(key))) {
      IOUtils.readFully(fsDataInputStream, read);
    }

    assertEquals(inputString, new String(read, StandardCharsets.UTF_8));
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
