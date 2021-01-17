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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ozone.OzoneFileSystem;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestObjectStoreV1 {

  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static String clusterId;
  private static String scmId;
  private static String omId;

  @Rule
  public Timeout timeout = new Timeout(240000);

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omId = UUID.randomUUID().toString();
    conf.set(OMConfigKeys.OZONE_OM_LAYOUT_VERSION, "V1");
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    cluster = MiniOzoneCluster.newBuilder(conf)
            .setClusterId(clusterId)
            .setScmId(scmId)
            .setOmId(omId)
            .build();
    cluster.waitForClusterToBeReady();
  }

  @Test
  public void testCreateKey() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String parent = "a/b/c/";
    String file = "key" + RandomStringUtils.randomNumeric(5);
    String key = parent + file;

    OzoneClient client = cluster.getClient();

    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(volumeName);

    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    Assert.assertTrue(ozoneVolume.getName().equals(volumeName));
    ozoneVolume.createBucket(bucketName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    Assert.assertTrue(ozoneBucket.getName().equals(bucketName));

    Table<String, OmKeyInfo> openKeyTable =
            cluster.getOzoneManager().getMetadataManager().getOpenKeyTable();

    // before file creation
    verifyKeyInFileTable(openKeyTable, file, 0, true);

    String data = "random data";
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(key,
            data.length(), ReplicationType.RATIS, ReplicationFactor.ONE,
            new HashMap<>());

    OmDirectoryInfo dirPathC = getDirInfo(volumeName, bucketName, parent);
    Assert.assertNotNull("Failed to find dir path: a/b/c", dirPathC);

    // after file creation
    verifyKeyInOpenFileTable(openKeyTable, file, dirPathC.getObjectID(),
            false);

    ozoneOutputStream.write(data.getBytes(), 0, data.length());
    ozoneOutputStream.close();

    Table<String, OmKeyInfo> keyTable =
            cluster.getOzoneManager().getMetadataManager().getKeyTable();

    // After closing the file. File entry should be removed from openFileTable
    // and it should be added to fileTable.
    verifyKeyInFileTable(keyTable, file, dirPathC.getObjectID(), false);
    verifyKeyInOpenFileTable(openKeyTable, file, dirPathC.getObjectID(),
            true);

    ozoneBucket.deleteKey(key);

    // after key delete
    verifyKeyInFileTable(keyTable, file, dirPathC.getObjectID(), true);
    verifyKeyInOpenFileTable(openKeyTable, file, dirPathC.getObjectID(),
            true);
  }

  @Test
  public void testLookupKey() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String parent = "a/b/c/";
    String file = "key" + RandomStringUtils.randomNumeric(5);
    String key = parent + file;

    OzoneClient client = cluster.getClient();

    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(volumeName);

    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    Assert.assertTrue(ozoneVolume.getName().equals(volumeName));
    ozoneVolume.createBucket(bucketName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    Assert.assertTrue(ozoneBucket.getName().equals(bucketName));

    Table<String, OmKeyInfo> openKeyTable =
            cluster.getOzoneManager().getMetadataManager().getOpenKeyTable();

    String data = "random data";
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(key,
            data.length(), ReplicationType.RATIS, ReplicationFactor.ONE,
            new HashMap<>());

    OmDirectoryInfo dirPathC = getDirInfo(volumeName, bucketName, parent);
    Assert.assertNotNull("Failed to find dir path: a/b/c", dirPathC);

    // after file creation
    verifyKeyInOpenFileTable(openKeyTable, file, dirPathC.getObjectID(),
            false);

    ozoneOutputStream.write(data.getBytes(), 0, data.length());

    // open key
    try {
      ozoneBucket.getKey(key);
      fail("Should throw exception as file is not visible and its still " +
              "open for writing!");
    } catch (OMException ome) {
      // expected
      assertEquals(ome.getResult(), OMException.ResultCodes.KEY_NOT_FOUND);
    }

    ozoneOutputStream.close();

    OzoneKeyDetails keyDetails = ozoneBucket.getKey(key);
    Assert.assertEquals(key, keyDetails.getName());

    Table<String, OmKeyInfo> keyTable =
            cluster.getOzoneManager().getMetadataManager().getKeyTable();

    // When closing the key, entry should be removed from openFileTable
    // and it should be added to fileTable.
    verifyKeyInFileTable(keyTable, file, dirPathC.getObjectID(), false);
    verifyKeyInOpenFileTable(openKeyTable, file, dirPathC.getObjectID(),
            true);

    ozoneBucket.deleteKey(key);

    // get deleted key
    try {
      ozoneBucket.getKey(key);
      fail("Should throw exception as file not exists!");
    } catch (OMException ome) {
      // expected
      assertEquals(ome.getResult(), OMException.ResultCodes.KEY_NOT_FOUND);
    }

    // after key delete
    verifyKeyInFileTable(keyTable, file, dirPathC.getObjectID(), true);
    verifyKeyInOpenFileTable(openKeyTable, file, dirPathC.getObjectID(),
            true);
  }

  @Test
  public void testListKeysWithNotNormalizedPath() throws Exception {
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    OzoneClient client = cluster.getClient();

    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(volumeName);

    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    Assert.assertTrue(ozoneVolume.getName().equals(volumeName));
    ozoneVolume.createBucket(bucketName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    Assert.assertTrue(ozoneBucket.getName().equals(bucketName));

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

    createKey(ozoneBucket, key1, 10, input, volumeName, bucketName);
    createKey(ozoneBucket, key2, 10, input, volumeName, bucketName);
    createKey(ozoneBucket, key3, 10, input, volumeName, bucketName);

    // Iterator with key name as prefix.

    Iterator<? extends OzoneKey> ozoneKeyIterator =
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
      byte[] input, String volumeName, String bucketName) throws Exception {

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
    String rootPath = String.format("%s://%s.%s/", OZONE_URI_SCHEME,
            bucketName, volumeName);
    OzoneFileSystem o3fs = (OzoneFileSystem) FileSystem.get(new URI(rootPath),
            conf);
    FSDataInputStream fsDataInputStream = o3fs.open(new Path(key));
    read = new byte[length];
    fsDataInputStream.read(read, 0, length);
    ozoneInputStream.close();

    Assert.assertEquals(inputString, new String(read));
  }

  private OmDirectoryInfo getDirInfo(String volumeName, String bucketName,
      String parentKey) throws Exception {
    OMMetadataManager omMetadataManager =
            cluster.getOzoneManager().getMetadataManager();
    long bucketId = TestOMRequestUtils.getBucketId(volumeName, bucketName,
            omMetadataManager);
    String[] pathComponents = StringUtils.split(parentKey, '/');
    long parentId = bucketId;
    OmDirectoryInfo dirInfo = null;
    for (int indx = 0; indx < pathComponents.length; indx++) {
      String pathElement = pathComponents[indx];
      String dbKey = omMetadataManager.getOzonePathKey(parentId,
              pathElement);
      dirInfo =
              omMetadataManager.getDirectoryTable().get(dbKey);
      parentId = dirInfo.getObjectID();
    }
    return dirInfo;
  }

  private void verifyKeyInFileTable(Table<String, OmKeyInfo> fileTable,
      String fileName, long parentID, boolean isEmpty) throws IOException {
    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> iterator
            = fileTable.iterator();

    if (isEmpty) {
      Assert.assertTrue("Table is not empty!", fileTable.isEmpty());
    } else {
      Assert.assertFalse("Table is empty!", fileTable.isEmpty());
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> next = iterator.next();
        Assert.assertEquals("Invalid Key: " + next.getKey(),
                parentID + "/" + fileName, next.getKey());
        OmKeyInfo omKeyInfo = next.getValue();
        Assert.assertEquals("Invalid Key", fileName,
                omKeyInfo.getFileName());
        Assert.assertEquals("Invalid Key", fileName,
                omKeyInfo.getKeyName());
        Assert.assertEquals("Invalid Key", parentID,
                omKeyInfo.getParentObjectID());
      }
    }
  }

  private void verifyKeyInOpenFileTable(Table<String, OmKeyInfo> openFileTable,
      String fileName, long parentID, boolean isEmpty) throws IOException {
    TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> iterator
            = openFileTable.iterator();

    if (isEmpty) {
      Assert.assertTrue("Table is not empty!",
              openFileTable.isEmpty());
    } else {
      Assert.assertFalse("Table is empty!", openFileTable.isEmpty());
      while (iterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> next = iterator.next();
        // used startsWith because the key format is,
        // <parentID>/fileName/<clientID> and clientID is not visible.
        Assert.assertTrue("Invalid Key: " + next.getKey(),
                next.getKey().startsWith(parentID + "/" + fileName));
        OmKeyInfo omKeyInfo = next.getValue();
        Assert.assertEquals("Invalid Key", fileName,
                omKeyInfo.getFileName());
        Assert.assertEquals("Invalid Key", fileName,
                omKeyInfo.getKeyName());
        Assert.assertEquals("Invalid Key", parentID,
                omKeyInfo.getParentObjectID());
      }
    }
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
