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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_WILDCARD;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PARTIAL_RENAME;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_ALREADY_EXISTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test some client operations after cluster starts. And perform restart and
 * then performs client operations and check the behavior is expected or not.
 */
public class TestOzoneManagerRestart {
  private static MiniOzoneCluster cluster = null;
  private static OzoneClient client;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ADMINISTRATORS, OZONE_ADMINISTRATORS_WILDCARD);
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);
    // Use OBS layout for key rename testing.
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.OBJECT_STORE.name());
    cluster =  MiniOzoneCluster.newBuilder(conf)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
  }

  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testRestartOMWithVolumeOperation() throws Exception {
    String volumeName = "volume" + RandomStringUtils.secure().nextNumeric(5);

    ObjectStore objectStore = client.getObjectStore();

    objectStore.createVolume(volumeName);

    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    assertEquals(volumeName, ozoneVolume.getName());

    cluster.restartOzoneManager();
    cluster.restartStorageContainerManager(true);

    // After restart, try to create same volume again, it should fail.
    OMException ome = assertThrows(OMException.class, () -> objectStore.createVolume(volumeName));
    assertEquals(VOLUME_ALREADY_EXISTS, ome.getResult());

    // Get Volume.
    ozoneVolume = objectStore.getVolume(volumeName);
    assertEquals(volumeName, ozoneVolume.getName());

  }

  @Test
  public void testRestartOMWithBucketOperation() throws Exception {
    String volumeName = "volume" + RandomStringUtils.secure().nextNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.secure().nextNumeric(5);

    ObjectStore objectStore = client.getObjectStore();

    objectStore.createVolume(volumeName);

    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    assertEquals(volumeName, ozoneVolume.getName());

    ozoneVolume.createBucket(bucketName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    assertEquals(bucketName, ozoneBucket.getName());

    cluster.restartOzoneManager();
    cluster.restartStorageContainerManager(true);

    // After restart, try to create same bucket again, it should fail.
    // After restart, try to create same volume again, it should fail.
    OMException ome = assertThrows(OMException.class, () -> ozoneVolume.createBucket(bucketName));
    assertEquals(BUCKET_ALREADY_EXISTS, ome.getResult());

    // Get bucket.
    ozoneBucket = ozoneVolume.getBucket(bucketName);
    assertEquals(bucketName, ozoneBucket.getName());
  }

  @Test
  public void testRestartOMWithKeyOperation() throws Exception {
    String volumeName = "volume" + RandomStringUtils.secure().nextNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.secure().nextNumeric(5);
    String key1 = "key1" + RandomStringUtils.secure().nextNumeric(5);
    String key2 = "key2" + RandomStringUtils.secure().nextNumeric(5);

    String newKey1 = "key1new" + RandomStringUtils.secure().nextNumeric(5);
    String newKey2 = "key2new" + RandomStringUtils.secure().nextNumeric(5);

    ObjectStore objectStore = client.getObjectStore();

    objectStore.createVolume(volumeName);

    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    assertEquals(volumeName, ozoneVolume.getName());

    ozoneVolume.createBucket(bucketName);

    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    assertEquals(bucketName, ozoneBucket.getName());

    String data = "random data";
    OzoneOutputStream ozoneOutputStream1 = ozoneBucket.createKey(key1,
        data.length(), ReplicationType.RATIS, ReplicationFactor.ONE,
        new HashMap<>());

    ozoneOutputStream1.write(data.getBytes(UTF_8), 0, data.length());
    ozoneOutputStream1.close();

    Map<String, String> keyMap = new HashMap();
    keyMap.put(key1, newKey1);
    keyMap.put(key2, newKey2);

    try {
      ozoneBucket.renameKeys(keyMap);
    } catch (OMException ex) {
      assertEquals(PARTIAL_RENAME, ex.getResult());
    }

    // Get original Key1, it should not exist
    try {
      ozoneBucket.getKey(key1);
    } catch (OMException ex) {
      assertEquals(KEY_NOT_FOUND, ex.getResult());
    }

    cluster.restartOzoneManager();
    cluster.restartStorageContainerManager(true);


    // As we allow override of keys, not testing re-create key. We shall see
    // after restart key exists or not.

    // Get newKey1.
    OzoneKey ozoneKey = ozoneBucket.getKey(newKey1);
    assertEquals(newKey1, ozoneKey.getName());
    assertEquals(ReplicationType.RATIS, ozoneKey.getReplicationType());

    // Get newKey2, it should not exist
    try {
      ozoneBucket.getKey(newKey2);
    } catch (OMException ex) {
      assertEquals(KEY_NOT_FOUND, ex.getResult());
    }
  }
}
