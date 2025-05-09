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

package org.apache.hadoop.fs.ozone;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConsts.ETAG;
import static org.apache.hadoop.ozone.OzoneConsts.MD5_HASH;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Class tests create with object store and getFileStatus.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestOzoneFSWithObjectStoreCreate implements NonHATests.TestCase {

  private OzoneClient client;
  private OzoneFileSystem o3fs;
  private String volumeName;
  private String bucketName;
  private OmConfig originalOmConfig;

  @BeforeAll
  void initClass() throws IOException {
    client = cluster().newClient();

    OmConfig omConfig = cluster().getOzoneManager().getConfig();
    originalOmConfig = omConfig.copy();
    omConfig.setFileSystemPathEnabled(true);

  }

  @AfterAll
  void tearDownClass() {
    IOUtils.closeQuietly(client);
    cluster().getOzoneManager().getConfig().setFrom(originalOmConfig);
  }

  @BeforeEach
  public void init() throws Exception {
    volumeName = RandomStringUtils.secure().nextAlphabetic(10).toLowerCase();
    bucketName = RandomStringUtils.secure().nextAlphabetic(10).toLowerCase();

    // create a volume and a bucket to be used by OzoneFileSystem
    TestDataUtil.createVolumeAndBucket(client, volumeName, bucketName, BucketLayout.LEGACY);

    String rootPath = String.format("%s://%s.%s/", OZONE_URI_SCHEME, bucketName,
        volumeName);
    o3fs = (OzoneFileSystem) FileSystem.get(new URI(rootPath), cluster().getConf());
  }

  @AfterEach
  void tearDown() {
    IOUtils.closeQuietly(o3fs);
  }

  @Test
  public void test() throws Exception {

    OzoneVolume ozoneVolume =
        client.getObjectStore().getVolume(volumeName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);

    String key1 = "///dir1/dir2/file1";
    String key2 = "///dir1/dir2/file2";
    int length = 10;
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(key1, length);
    byte[] b = new byte[10];
    Arrays.fill(b, (byte)96);
    ozoneOutputStream.write(b);
    ozoneOutputStream.close();

    ozoneOutputStream = ozoneBucket.createKey(key2, length);
    ozoneOutputStream.write(b);
    ozoneOutputStream.close();

    // Adding "/" here otherwise Path will be considered as relative path and
    // workingDir will be added.
    key1 = "///dir1/dir2/file1";
    Path p = new Path(key1);
    assertTrue(o3fs.getFileStatus(p).isFile());

    p = p.getParent();
    checkAncestors(p);


    key2 = "///dir1/dir2/file2";
    p = new Path(key2);
    assertTrue(o3fs.getFileStatus(p).isFile());
    checkAncestors(p);

  }

  @Test
  public void testObjectStoreCreateWithO3fs() throws Exception {
    OzoneVolume ozoneVolume =
        client.getObjectStore().getVolume(volumeName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);


    // Use ObjectStore API to create keys. This similates how s3 create keys.
    String parentDir = "/dir1/dir2/dir3/dir4/";


    List<String> keys = new ArrayList<>();
    keys.add("/dir1");
    keys.add("/dir1/dir2");
    keys.add("/dir1/dir2/dir3");
    keys.add("/dir1/dir2/dir3/dir4/");
    for (int i = 1; i <= 3; i++) {
      int length = 10;
      String fileName = parentDir.concat("/file" + i + "/");
      keys.add(fileName);
      OzoneOutputStream ozoneOutputStream =
          ozoneBucket.createKey(fileName, length);
      byte[] b = new byte[10];
      Arrays.fill(b, (byte)96);
      ozoneOutputStream.write(b);
      ozoneOutputStream.close();
    }

    // check
    for (int i = 1; i <= 3; i++) {
      String fileName = parentDir.concat("/file" + i + "/");
      Path p = new Path(fileName);
      assertTrue(o3fs.getFileStatus(p).isFile());
      checkAncestors(p);
    }

    // Delete keys with object store api delete
    for (int i = 1; i <= 3; i++) {
      String fileName = parentDir.concat("/file" + i + "/");
      ozoneBucket.deleteKey(fileName);
    }


    // Delete parent dir via o3fs.
    boolean result = o3fs.delete(new Path("/dir1"), true);
    assertTrue(result);

    // No Key should exist.
    for (String key : keys) {
      checkPath(new Path(key));
    }


    for (int i = 1; i <= 3; i++) {
      int length = 10;
      String fileName = parentDir.concat("/file" + i + "/");
      OzoneOutputStream ozoneOutputStream =
          ozoneBucket.createKey(fileName, length);
      byte[] b = new byte[10];
      Arrays.fill(b, (byte)96);
      ozoneOutputStream.write(b);
      ozoneOutputStream.close();
    }

    o3fs.mkdirs(new Path("/dest"));
    o3fs.rename(new Path("/dir1"), new Path("/dest"));

    // No source Key should exist.
    for (String key : keys) {
      checkPath(new Path(key));
    }

    // check dest path.
    for (int i = 1; i <= 3; i++) {
      String fileName = "/dest/".concat(parentDir.concat("/file" + i + "/"));
      Path p = new Path(fileName);
      assertTrue(o3fs.getFileStatus(p).isFile());
      checkAncestors(p);
    }

  }

  @Test
  public void testKeyCreationFailDuetoDirectoryCreationBeforeCommit()
      throws Exception {
    OzoneVolume ozoneVolume =
        client.getObjectStore().getVolume(volumeName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);

    OzoneOutputStream ozoneOutputStream =
        ozoneBucket.createKey("/a/b/c", 10);
    byte[] b = new byte[10];
    Arrays.fill(b, (byte)96);
    ozoneOutputStream.write(b);

    // Before close create directory with same name.
    o3fs.mkdirs(new Path("/a/b/c"));
    OMException ex = assertThrows(OMException.class, () -> ozoneOutputStream.close());
    assertEquals(NOT_A_FILE, ex.getResult());
  }

  @Test
  public void testMPUFailDuetoDirectoryCreationBeforeComplete()
      throws Exception {
    OzoneVolume ozoneVolume =
        client.getObjectStore().getVolume(volumeName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);

    String keyName = "/dir1/dir2/mpukey";
    OmMultipartInfo omMultipartInfo =
        ozoneBucket.initiateMultipartUpload(keyName);
    assertNotNull(omMultipartInfo);

    OzoneOutputStream ozoneOutputStream =
        ozoneBucket.createMultipartKey(keyName, 10, 1,
            omMultipartInfo.getUploadID());
    byte[] b = new byte[10];
    Arrays.fill(b, (byte)96);
    ozoneOutputStream.write(b);

    // Before close, create directory with same name.
    o3fs.mkdirs(new Path(keyName));

    // This should succeed, as we check during creation of part or during
    // complete MPU.
    ozoneOutputStream.getMetadata().put(ETAG,
        DatatypeConverter.printHexBinary(MessageDigest.getInstance(MD5_HASH)
            .digest(b)).toLowerCase());
    ozoneOutputStream.close();

    Map<Integer, String> partsMap = new HashMap<>();
    partsMap.put(1, ozoneOutputStream.getCommitUploadPartInfo().getETag());

    // Should fail, as we have directory with same name.
    OMException ex = assertThrows(OMException.class, () -> ozoneBucket.completeMultipartUpload(keyName,
        omMultipartInfo.getUploadID(), partsMap));
    assertEquals(NOT_A_FILE, ex.getResult());


    // Delete directory
    o3fs.delete(new Path(keyName), true);

    // And try again for complete MPU. This should succeed.
    ozoneBucket.completeMultipartUpload(keyName,
        omMultipartInfo.getUploadID(), partsMap);

    try (FSDataInputStream ozoneInputStream = o3fs.open(new Path(keyName))) {
      byte[] buffer = new byte[10];
      // This read will not change the offset inside the file
      int readBytes = ozoneInputStream.read(0, buffer, 0, 10);
      String readData = new String(buffer, 0, readBytes, UTF_8);
      assertEquals(new String(b, 0, b.length, UTF_8), readData);
    }

  }

  @Test
  public void testCreateDirectoryFirstThenKeyAndFileWithSameName()
      throws Exception {
    o3fs.mkdirs(new Path("/t1/t2"));
    FileAlreadyExistsException e =
        assertThrows(FileAlreadyExistsException.class, () -> o3fs.create(new Path("/t1/t2")));
    assertThat(e.getMessage()).contains(NOT_A_FILE.name());

    OzoneVolume ozoneVolume =
        client.getObjectStore().getVolume(volumeName);
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    ozoneBucket.createDirectory("t1/t2");
    OMException ex = assertThrows(OMException.class, () -> ozoneBucket.createKey("t1/t2", 0));
    assertEquals(NOT_A_FILE, ex.getResult());
  }

  @Test
  public void testListKeysWithNotNormalizedPath() throws Exception {

    OzoneVolume ozoneVolume =
        client.getObjectStore().getVolume(volumeName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);

    String key1 = "/dir1///dir2/file1/";
    String key2 = "/dir1///dir2/file2/";
    String key3 = "/dir1///dir2/file3/";

    LinkedList<String> keys = new LinkedList<>();
    keys.add("dir1/");
    keys.add("dir1/dir2/");
    keys.add(OmUtils.normalizeKey(key1, false));
    keys.add(OmUtils.normalizeKey(key2, false));
    keys.add(OmUtils.normalizeKey(key3, false));

    createAndAssertKey(ozoneBucket, key1, 10);
    createAndAssertKey(ozoneBucket, key2, 10);
    createAndAssertKey(ozoneBucket, key3, 10);

    // Iterator with key name as prefix.

    Iterator<? extends OzoneKey > ozoneKeyIterator =
        ozoneBucket.listKeys("/dir1//");

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

  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4})
  public void testDoubleSlashPrefixPathNormalization(int slashCount) throws Exception {
    OzoneVolume ozoneVolume = client.getObjectStore().getVolume(volumeName);
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    // Generate a path with the specified number of leading slashes
    StringBuilder keyPrefix = new StringBuilder();
    for (int i = 0; i < slashCount; i++) {
      keyPrefix.append('/');
    }
    String dirPath = "dir" + slashCount + "/";
    String keyName = "key" + slashCount;
    String slashyKey = keyPrefix + dirPath + keyName;
    String normalizedKey = dirPath + keyName;
    byte[] data = new byte[10];
    Arrays.fill(data, (byte)96);
    ArrayList<String> expectedKeys = new ArrayList<>();
    expectedKeys.add(dirPath);
    expectedKeys.add(normalizedKey);
    TestDataUtil.createKey(ozoneBucket, slashyKey, data);

    try {
      ozoneBucket.readKey(slashyKey).close();
      ozoneBucket.readKey(normalizedKey).close();
    } catch (Exception e) {
      fail("Should be able to read key " + e.getMessage());
    }

    Iterator<? extends OzoneKey> it = ozoneBucket.listKeys(dirPath);
    checkKeyList(it, expectedKeys);
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

  private void createAndAssertKey(OzoneBucket ozoneBucket, String key, int length)
      throws Exception {
    
    byte[] input = TestDataUtil.createStringKey(ozoneBucket, key, length);
    // Read the key with given key name.
    readKey(ozoneBucket, key, length, input);

  }

  private void readKey(OzoneBucket ozoneBucket, String key, int length, byte[] input)
      throws Exception {

    byte[] read = new byte[length];
    try (InputStream in = ozoneBucket.readKey(key)) {
      IOUtils.readFully(in, read);
    }

    String inputString = new String(input, UTF_8);
    assertEquals(inputString, new String(read, UTF_8));

    // Read using filesystem.
    try (InputStream in = o3fs.open(new Path(key))) {
      IOUtils.readFully(in, read);
    }

    assertEquals(inputString, new String(read, UTF_8));
  }

  private void checkPath(Path path) {
    FileNotFoundException ex = assertThrows(FileNotFoundException.class, () ->
        o3fs.getFileStatus(path),
        "testObjectStoreCreateWithO3fs failed for Path" + path);
    assertThat(ex.getMessage()).contains("No such file or directory");
  }

  private void checkAncestors(Path p) throws Exception {
    p = p.getParent();
    while (p.getParent() != null) {
      FileStatus fileStatus = o3fs.getFileStatus(p);
      assertTrue(fileStatus.isDirectory());
      p = p.getParent();
    }
  }

}
