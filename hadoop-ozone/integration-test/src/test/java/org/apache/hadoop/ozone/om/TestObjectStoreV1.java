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
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestObjectStoreV1 {

  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static String clusterId;
  private static String scmId;
  private static String omId;
  private static String volumeName;
  private static String bucketName;
  private static FileSystem fs;

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
    cluster = MiniOzoneCluster.newBuilder(conf)
            .setClusterId(clusterId)
            .setScmId(scmId)
            .setOmId(omId)
            .build();
    cluster.waitForClusterToBeReady();
    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster);
    volumeName = bucket.getVolumeName();
    bucketName = bucket.getName();

    String rootPath = String.format("%s://%s.%s/",
            OzoneConsts.OZONE_URI_SCHEME, bucket.getName(),
            bucket.getVolumeName());
    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);
    fs = FileSystem.get(conf);
  }

  @After
  public void tearDown() throws Exception {
    deleteRootDir();
  }

  /**
   * Cleanup files and directories.
   *
   * @throws IOException DB failure
   */
  private void deleteRootDir() throws IOException {
    Path root = new Path("/");
    FileStatus[] fileStatuses = fs.listStatus(root);

    if (fileStatuses == null) {
      return;
    }

    for (FileStatus fStatus : fileStatuses) {
      fs.delete(fStatus.getPath(), true);
    }

    fileStatuses = fs.listStatus(root);
    if (fileStatuses != null) {
      Assert.assertEquals("Delete root failed!", 0, fileStatuses.length);
    }
  }

  @Test
  public void testCreateKey() throws Exception {
    String parent = "a/b/c/";
    String file = "key" + RandomStringUtils.randomNumeric(5);
    String key = parent + file;

    OzoneClient client = cluster.getClient();

    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    Assert.assertTrue(ozoneVolume.getName().equals(volumeName));
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    Assert.assertTrue(ozoneBucket.getName().equals(bucketName));

    Table<String, OmKeyInfo> openFileTable =
            cluster.getOzoneManager().getMetadataManager().getOpenKeyTable();

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
    Assert.assertNotNull("Failed to find dir path: a/b/c", dirPathC);

    // after file creation
    verifyKeyInOpenFileTable(openFileTable, clientID, file,
            dirPathC.getObjectID(), false);

    ozoneOutputStream.write(data.getBytes(), 0, data.length());
    ozoneOutputStream.close();

    Table<String, OmKeyInfo> fileTable =
            cluster.getOzoneManager().getMetadataManager().getKeyTable();
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

  @Test
  public void testLookupKey() throws Exception {
    String parent = "a/b/c/";
    String fileName = "key" + RandomStringUtils.randomNumeric(5);
    String key = parent + fileName;

    OzoneClient client = cluster.getClient();

    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    Assert.assertTrue(ozoneVolume.getName().equals(volumeName));
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    Assert.assertTrue(ozoneBucket.getName().equals(bucketName));

    Table<String, OmKeyInfo> openFileTable =
            cluster.getOzoneManager().getMetadataManager().getOpenKeyTable();

    String data = "random data";
    OzoneOutputStream ozoneOutputStream = ozoneBucket.createKey(key,
            data.length(), ReplicationType.RATIS, ReplicationFactor.ONE,
            new HashMap<>());

    KeyOutputStream keyOutputStream =
            (KeyOutputStream) ozoneOutputStream.getOutputStream();
    long clientID = keyOutputStream.getClientID();

    OmDirectoryInfo dirPathC = getDirInfo(parent);
    Assert.assertNotNull("Failed to find dir path: a/b/c", dirPathC);

    // after file creation
    verifyKeyInOpenFileTable(openFileTable, clientID, fileName,
            dirPathC.getObjectID(), false);

    ozoneOutputStream.write(data.getBytes(), 0, data.length());

    // open key
    try {
      ozoneBucket.getKey(key);
      fail("Should throw exception as fileName is not visible and its still " +
              "open for writing!");
    } catch (OMException ome) {
      // expected
      assertEquals(ome.getResult(), OMException.ResultCodes.KEY_NOT_FOUND);
    }

    ozoneOutputStream.close();

    OzoneKeyDetails keyDetails = ozoneBucket.getKey(key);
    Assert.assertEquals(key, keyDetails.getName());

    Table<String, OmKeyInfo> fileTable =
            cluster.getOzoneManager().getMetadataManager().getKeyTable();

    // When closing the key, entry should be removed from openFileTable
    // and it should be added to fileTable.
    verifyKeyInFileTable(fileTable, fileName, dirPathC.getObjectID(), false);
    verifyKeyInOpenFileTable(openFileTable, clientID, fileName,
            dirPathC.getObjectID(), true);

    ozoneBucket.deleteKey(key);

    // get deleted key
    try {
      ozoneBucket.getKey(key);
      fail("Should throw exception as fileName not exists!");
    } catch (OMException ome) {
      // expected
      assertEquals(ome.getResult(), OMException.ResultCodes.KEY_NOT_FOUND);
    }

    // after key delete
    verifyKeyInFileTable(fileTable, fileName, dirPathC.getObjectID(), true);
    verifyKeyInOpenFileTable(openFileTable, clientID, fileName,
            dirPathC.getObjectID(), true);
  }

  private OmDirectoryInfo getDirInfo(String parentKey) throws Exception {
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

    String dbFileKey = parentID + OM_KEY_PREFIX + fileName;
    OmKeyInfo omKeyInfo = fileTable.get(dbFileKey);
    if (isEmpty) {
      Assert.assertNull("Table is not empty!", omKeyInfo);
    } else {
      Assert.assertNotNull("Table is empty!", omKeyInfo);
      // used startsWith because the key format is,
      // <parentID>/fileName/<clientID> and clientID is not visible.
      Assert.assertEquals("Invalid Key: " + omKeyInfo.getObjectInfo(),
              omKeyInfo.getKeyName(), fileName);
      Assert.assertEquals("Invalid Key", parentID,
              omKeyInfo.getParentObjectID());
    }
  }

  private void verifyKeyInOpenFileTable(Table<String, OmKeyInfo> openFileTable,
      long clientID, String fileName, long parentID, boolean isEmpty)
          throws IOException {
    String dbOpenFileKey =
            parentID + OM_KEY_PREFIX + fileName + OM_KEY_PREFIX + clientID;
    OmKeyInfo omKeyInfo = openFileTable.get(dbOpenFileKey);
    if (isEmpty) {
      Assert.assertNull("Table is not empty!", omKeyInfo);
    } else {
      Assert.assertNotNull("Table is empty!", omKeyInfo);
      // used startsWith because the key format is,
      // <parentID>/fileName/<clientID> and clientID is not visible.
      Assert.assertEquals("Invalid Key: " + omKeyInfo.getObjectInfo(),
              omKeyInfo.getKeyName(), fileName);
      Assert.assertEquals("Invalid Key", parentID,
              omKeyInfo.getParentObjectID());
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
