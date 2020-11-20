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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Test;

/**
 * Test OM prepare against actual mini cluster.
 */
public class TestOzoneManagerPrepare extends TestOzoneManagerHA {

  /**
   * Calls prepare on the leader OM which has no transaction information.
   * Checks that it is brought into prepare mode successfully.
   */
  @Test
  public void testPrepareWithoutTransactions() {

  }

  /**
   * Writes data to the cluster via the leader OM, and then prepares it.
   * Checks that the OM is prepared successfully.
   */
  @Test
  public void testPrepareWithTransactions() throws Exception {

  }

  /**
   * Writes data to the cluster.
   * Shuts down one OM.
   * Writes more data to the cluster.
   * Submits prepare as ratis request.
   * Checks that two live OMs are prepared.
   * Revives the third OM
   * Checks that third OM received all transactions and is prepared.
   * @throws Exception
   */
  @Test
  public void testPrepareDownedOM() throws Exception {
    // Index of the OM that will be shut down during this test.
    final int shutdownOMIndex = 1;

    MiniOzoneHAClusterImpl cluster = getCluster();
    OzoneClient ozClient = OzoneClientFactory.getRpcClient(getConf());

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    ObjectStore store = ozClient.getObjectStore();

    // Create keys with all 3 OMs up.
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);

    for (int i = 1; i <= 50; i++) {
      String keyName = "Test-Key-" + i;
      writeTestData(store, volumeName, bucketName, keyName);
    }

    // Shut down one OM.
    cluster.stopOzoneManager(shutdownOMIndex);
    OzoneManager downedOM = cluster.getOzoneManager(shutdownOMIndex);

    // Write keys with the remaining OMs up.
    for (int i = 51; i <= 100; i++) {
      String keyName = "Test-Key-" + i;
      writeTestData(store, volumeName, bucketName, keyName);
      assertNotNull(getObjectStore().getVolume(volumeName)
          .getBucket(bucketName)
          .getKey(keyName));
    }

    // Check that running OMs have log data.
    for (int i = 0; i < cluster.getOzoneManagersList().size(); i++) {
      if (i != shutdownOMIndex) {
        OzoneManager om = cluster.getOzoneManager(i);
        assertTrue(logFilesPresentInRatisPeer(om));
        assertFalse(om.isPrepared());
      }
    }

    // Submit prepare request via Ratis.
    OzoneManager leaderOM = cluster.getOzoneManager();
    // TODO: Check index of response.
    leaderOM.getOmRatisServer().submitRequest(buildPrepareRequest());

    // Check that the two live OMs are prepared.
    for (int i = 0; i < cluster.getOzoneManagersList().size(); i++) {
      if (i != shutdownOMIndex) {
        OzoneManager om = cluster.getOzoneManager(i);
        // Wait for Ratis request to complete on this OM.
        GenericTestUtils.waitFor(om::isPrepared, 500, 3000);
        assertFalse(logFilesPresentInRatisPeer(om));
        // TODO: Check snapshot index.
      }
    }

    // Restart the downed OM and wait for it to catch up.
    // Since prepare was the last Ratis transaction, it should have all data
    // it missed once it receives the prepare transaction.
    cluster.restartOzoneManager(downedOM, true);
    GenericTestUtils.waitFor(downedOM::isPrepared, 500, 3000);
    assertFalse(logFilesPresentInRatisPeer(downedOM));
    // TODO: Check snapshot index.
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

  private OzoneManagerProtocolProtos.OMRequest buildPrepareRequest() {
    OzoneManagerProtocolProtos.PrepareForUpgradeRequest requestProto =
        OzoneManagerProtocolProtos.PrepareForUpgradeRequest.newBuilder().build();

    OzoneManagerProtocolProtos.OMRequest omRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setPrepareForUpgradeRequest(requestProto)
        .setCmdType(OzoneManagerProtocolProtos.Type.PrepareForUpgrade)
        .setClientId(UUID.randomUUID().toString())
        .build();

    return omRequest;
  }

}