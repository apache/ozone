/*
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Test;

/**
 * Test OM prepare against actual mini cluster.
 */
public class TestOzoneManagerPrepare extends TestOzoneManagerHA {

  @Test
  public void testPrepare() throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    OzoneClient ozClient = OzoneClientFactory.getRpcClient(getConf());

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    ObjectStore store = ozClient.getObjectStore();
    String keyName = "Test-Key-1";

//    // Create a key with all 3 OMs up.
//    store.createVolume(volumeName);
//    OzoneVolume volume = store.getVolume(volumeName);
//    volume.createBucket(bucketName);

//    for (int i = 1; i <= 50; i++) {
//      keyName = "Test-Key-" + i;
//      writeTestData(store, volumeName, bucketName, keyName);
//    }

//    // Shutdown any random OM.
//    int index = new Random().nextInt(3);
//    cluster.stopOzoneManager(index);
//    OzoneManager downedOM = cluster.getOzoneManager(index);
//
//    // Write a Key with the remaining OMs up.
//    for (int i = 51; i <= 100; i++) {
//      keyName = "Test-Key-" + i;
//      writeTestData(store, volumeName, bucketName, keyName);
//      assertNotNull(getObjectStore().getVolume(volumeName)
//          .getBucket(bucketName)
//          .getKey(keyName));
//    }

    // TODO: Get log index to check snapshot.

    for (int i = 0; i < cluster.getOzoneManagersList().size(); i++) {
      OzoneManager om = cluster.getOzoneManager(i);
      assertTrue(logFilesPresentInRatisPeer(om));
      assertFalse(om.isPrepared());
      om.prepare();
      assertTrue(om.isPrepared());
      assertFalse(logFilesPresentInRatisPeer(om));
    }

//    // Restart the downed OM and wait for it to catch up.
//    cluster.restartOzoneManager(downedOM, true);
//    String finalKeyName = keyName;
//    LambdaTestUtils.await(20000, 5000, () -> {
//      OMMetadataManager metadataManager = downedOM.getMetadataManager();
//      String ozoneKey =
//          metadataManager.getOzoneKey(volumeName, bucketName, finalKeyName);
//      return (metadataManager.getKeyTable().isExist(ozoneKey));
//    });
//
//    // Make sure downed OM has all data.
//    for (int i = 1; i <= 100; i++) {
//      OMMetadataManager metadataManager = downedOM.getMetadataManager();
//      String ozoneKey =
//          metadataManager.getOzoneKey(volumeName, bucketName, finalKeyName);
//      assertTrue(metadataManager.getKeyTable().isExist(ozoneKey));
//    }
  }

  private boolean logFilesPresentInRatisPeer(OzoneManager om) {
    String ratisDir = om.getOmRatisServer().getServer().getProperties()
        .get("raft.server.storage.dir");
    String groupIdDirName =
        om.getOmRatisServer().getServer().getGroupIds().iterator()
            .next().getUuid().toString();
    File logDir = Paths.get(ratisDir, groupIdDirName, "current")
        .toFile();

    for (File file : logDir.listFiles()) {
      if (file.getName().startsWith("log")) {
        return true;
      }
    }
    return false;
  }

  private void writeTestData(ObjectStore store, String volumeName,
                             String bucketName, String keyName)
      throws Exception {
    String keyString = UUID.randomUUID().toString();
    byte[] data = ContainerTestHelper.getFixedLengthString(
        keyString, 100).getBytes(UTF_8);
    OzoneOutputStream keyStream = TestHelper.createKey(
        keyName, ReplicationType.STAND_ALONE, ReplicationFactor.ONE,
        100, store, volumeName, bucketName);
    keyStream.write(data);
    keyStream.close();
  }

}