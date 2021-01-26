/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_A_FILE;
import static org.junit.Assert.fail;

/**
 * Class tests create with object store and getFileStatus.
 */
public class TestOzoneFSWithObjectStoreCreate {

  @Rule
  public Timeout timeout = new Timeout(300000);

  private String rootPath;

  private MiniOzoneCluster cluster = null;

  private OzoneFileSystem o3fs;

  private String volumeName;

  private String bucketName;


  @Before
  public void init() throws Exception {
    volumeName = RandomStringUtils.randomAlphabetic(10).toLowerCase();
    bucketName = RandomStringUtils.randomAlphabetic(10).toLowerCase();

    OzoneConfiguration conf = new OzoneConfiguration();

    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();

    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket =
        TestDataUtil.createVolumeAndBucket(cluster, volumeName, bucketName);

    rootPath = String.format("%s://%s.%s/", OZONE_URI_SCHEME, bucketName,
        volumeName);
    o3fs = (OzoneFileSystem) FileSystem.get(new URI(rootPath), conf);
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(o3fs);
  }

  @Test
  public void test() throws Exception {

    OzoneVolume ozoneVolume =
        cluster.getRpcClient().getObjectStore().getVolume(volumeName);

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
    Assert.assertTrue(o3fs.getFileStatus(p).isFile());

    p = p.getParent();
    checkAncestors(p);


    key2 = "///dir1/dir2/file2";
    p = new Path(key2);
    Assert.assertTrue(o3fs.getFileStatus(p).isFile());
    checkAncestors(p);

  }


  @Test
  public void testObjectStoreCreateWithO3fs() throws Exception {
    OzoneVolume ozoneVolume =
        cluster.getRpcClient().getObjectStore().getVolume(volumeName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);


    // Use ObjectStore API to create keys. This similates how s3 create keys.
    String parentDir = "/dir1/dir2/dir3/dir4/";


    List<String> keys = new ArrayList<>();
    keys.add("/dir1");
    keys.add("/dir1/dir2");
    keys.add("/dir1/dir2/dir3");
    keys.add("/dir1/dir2/dir3/dir4/");
    for (int i=1; i <= 3; i++) {
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
    for (int i=1; i <= 3; i++) {
      String fileName = parentDir.concat("/file" + i + "/");
      Path p = new Path(fileName);
      Assert.assertTrue(o3fs.getFileStatus(p).isFile());
      checkAncestors(p);
    }

    // Delete keys with object store api delete
    for (int i = 1; i <= 3; i++) {
      String fileName = parentDir.concat("/file" + i + "/");
      ozoneBucket.deleteKey(fileName);
    }


    // Delete parent dir via o3fs.
    boolean result = o3fs.delete(new Path("/dir1"), true);
    Assert.assertTrue(result);

    // No Key should exist.
    for(String key : keys) {
      checkPath(new Path(key));
    }


    for (int i=1; i <= 3; i++) {
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
    for(String key : keys) {
      checkPath(new Path(key));
    }

    // check dest path.
    for (int i=1; i <= 3; i++) {
      String fileName = "/dest/".concat(parentDir.concat("/file" + i + "/"));
      Path p = new Path(fileName);
      Assert.assertTrue(o3fs.getFileStatus(p).isFile());
      checkAncestors(p);
    }

  }


  @Test
  public void testKeyCreationFailDuetoDirectoryCreationBeforeCommit()
      throws Exception {
    OzoneVolume ozoneVolume =
        cluster.getRpcClient().getObjectStore().getVolume(volumeName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);

    OzoneOutputStream ozoneOutputStream =
        ozoneBucket.createKey("/a/b/c", 10);
    byte[] b = new byte[10];
    Arrays.fill(b, (byte)96);
    ozoneOutputStream.write(b);

    // Before close create directory with same name.
    o3fs.mkdirs(new Path("/a/b/c"));

    try {
      ozoneOutputStream.close();
      fail("testKeyCreationFailDuetoDirectoryCreationBeforeCommit");
    } catch (IOException ex) {
      Assert.assertTrue(ex instanceof OMException);
      Assert.assertEquals(NOT_A_FILE,
          ((OMException) ex).getResult());
    }

  }


  @Test
  public void testMPUFailDuetoDirectoryCreationBeforeComplete()
      throws Exception {
    OzoneVolume ozoneVolume =
        cluster.getRpcClient().getObjectStore().getVolume(volumeName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);

    String keyName = "/dir1/dir2/mpukey";
    OmMultipartInfo omMultipartInfo =
        ozoneBucket.initiateMultipartUpload(keyName);
    Assert.assertNotNull(omMultipartInfo);

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
    ozoneOutputStream.close();

    Map<Integer, String> partsMap = new HashMap<>();
    partsMap.put(1, ozoneOutputStream.getCommitUploadPartInfo().getPartName());

    // Should fail, as we have directory with same name.
    try {
      ozoneBucket.completeMultipartUpload(keyName,
          omMultipartInfo.getUploadID(), partsMap);
      fail("testMPUFailDuetoDirectoryCreationBeforeComplete failed");
    } catch (OMException ex) {
      Assert.assertTrue(ex instanceof OMException);
      Assert.assertEquals(NOT_A_FILE, ex.getResult());
    }

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
      Assert.assertEquals(new String(b, 0, b.length, UTF_8), readData);
    }

  }

  @Test
  public void testCreateDirectoryFirstThenKeyAndFileWithSameName()
      throws Exception {
    o3fs.mkdirs(new Path("/t1/t2"));

    try {
      o3fs.create(new Path("/t1/t2"));
      fail("testCreateDirectoryFirstThenFileWithSameName failed");
    } catch (FileAlreadyExistsException ex) {
      Assert.assertTrue(ex.getMessage().contains(NOT_A_FILE.name()));
    }

    OzoneVolume ozoneVolume =
        cluster.getRpcClient().getObjectStore().getVolume(volumeName);
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    ozoneBucket.createDirectory("t1/t2");
    try {
      ozoneBucket.createKey("t1/t2", 0);
      fail("testCreateDirectoryFirstThenFileWithSameName failed");
    } catch (OMException ex) {
      Assert.assertTrue(ex instanceof OMException);
      Assert.assertEquals(NOT_A_FILE, ex.getResult());
    }
  }


  @Test
  public void testListKeysWithNotNormalizedPath() throws Exception {

    OzoneVolume ozoneVolume =
        cluster.getRpcClient().getObjectStore().getVolume(volumeName);

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

    int length = 10;
    byte[] input = new byte[length];
    Arrays.fill(input, (byte)96);

    createKey(ozoneBucket, key1, 10, input);
    createKey(ozoneBucket, key2, 10, input);
    createKey(ozoneBucket, key3, 10, input);

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

  private void checkKeyList(Iterator<? extends OzoneKey > ozoneKeyIterator,
      List<String> keys) {

    LinkedList<String> outputKeys = new LinkedList<>();
    while (ozoneKeyIterator.hasNext()) {
      OzoneKey ozoneKey = ozoneKeyIterator.next();
      outputKeys.add(ozoneKey.getName());
    }

    Assert.assertEquals(keys, outputKeys);
  }

  private void createKey(OzoneBucket ozoneBucket, String key, int length,
      byte[] input)
      throws Exception {

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

    String inputString = new String(input);
    Assert.assertEquals(inputString, new String(read));

    // Read using filesystem.
    FSDataInputStream fsDataInputStream = o3fs.open(new Path(key));
    read = new byte[length];
    fsDataInputStream.read(read, 0, length);
    ozoneInputStream.close();

    Assert.assertEquals(inputString, new String(read));
  }

  private void checkPath(Path path) {
    try {
      o3fs.getFileStatus(path);
      fail("testObjectStoreCreateWithO3fs failed for Path" + path);
    } catch (IOException ex) {
      Assert.assertTrue(ex instanceof FileNotFoundException);
      Assert.assertTrue(ex.getMessage().contains("No such file or directory"));
    }
  }

  private void checkAncestors(Path p) throws Exception {
    p = p.getParent();
    while(p.getParent() != null) {
      FileStatus fileStatus = o3fs.getFileStatus(p);
      Assert.assertTrue(fileStatus.isDirectory());
      p = p.getParent();
    }
  }

}
