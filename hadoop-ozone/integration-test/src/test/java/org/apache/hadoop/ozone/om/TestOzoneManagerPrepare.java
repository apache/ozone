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
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;

import java.io.File;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.ratis.OMTransactionInfo;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test OM prepare against actual mini cluster.
 */
public class TestOzoneManagerPrepare extends TestOzoneManagerHA {

  private final String keyPrefix = "key";
  private final int timeoutMillis = 30000;
  private final static Long PREPARE_FLUSH_WAIT_TIMEOUT_SECONDS = 300L;
  private final static Long PREPARE_FLUSH_INTERVAL_SECONDS = 5L;

  /**
   * Calls prepare on all OMs when they have no transaction information.
   * Checks that they are brought into prepare mode successfully.
   */
  @Test
  public void testPrepareWithoutTransactions() throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    ClientProtocol ozClient = OzoneClientFactory.getRpcClient(getConf())
        .getObjectStore().getClientProxy();
    long prepareRequestLogIndex =
        ozClient.getOzoneManagerClient().prepareOzoneManager(
            PREPARE_FLUSH_WAIT_TIMEOUT_SECONDS, PREPARE_FLUSH_INTERVAL_SECONDS);

    // Prepare response processing is included in the snapshot,
    // giving index of 1.
    Assert.assertEquals(1, prepareRequestLogIndex);
    for (OzoneManager om: cluster.getOzoneManagersList()) {
      waitAndCheckPrepared(om, prepareRequestLogIndex);
    }
  }

  /**
   * Writes data to the cluster via the leader OM, and then prepares it.
   * Checks that every OM is prepared successfully.
   */
  @Test
  public void testPrepareWithTransactions() throws Exception {
    MiniOzoneHAClusterImpl cluster = getCluster();
    OzoneClient ozClient = OzoneClientFactory.getRpcClient(getConf());

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    ObjectStore store = ozClient.getObjectStore();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);

    Set<String> writtenKeys = new HashSet<>();
    for (int i = 1; i <= 10; i++) {
      String keyName = keyPrefix + i;
      writeTestData(store, volumeName, bucketName, keyName);
      writtenKeys.add(keyName);
    }

    // Make sure all OMs have logs from writing data, so we can check that
    // they are purged after prepare.
    for (OzoneManager om: cluster.getOzoneManagersList()) {
      LambdaTestUtils.await(timeoutMillis, 1000,
          () -> logFilesPresentInRatisPeer(om));
    }

    OzoneManagerProtocol ozoneManagerClient =
        ozClient.getObjectStore().getClientProxy().getOzoneManagerClient();
    long prepareRequestLogIndex =
        ozoneManagerClient.prepareOzoneManager(
            PREPARE_FLUSH_WAIT_TIMEOUT_SECONDS, PREPARE_FLUSH_INTERVAL_SECONDS);

    // Make sure all OMs are prepared and all OMs still have their data.
    for (OzoneManager om: cluster.getOzoneManagersList()) {
      waitAndCheckPrepared(om, prepareRequestLogIndex);
      List<OmKeyInfo> keys = om.getMetadataManager().listKeys(volumeName,
          bucketName, null, keyPrefix, 100);

      Assert.assertEquals(writtenKeys.size(), keys.size());
      for (OmKeyInfo keyInfo: keys) {
        Assert.assertTrue(writtenKeys.contains(keyInfo.getKeyName()));
      }
    }
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
//  @Test
  public void testPrepareDownedOM() throws Exception {
    // Index of the OM that will be shut down during this test.
    final int shutdownOMIndex = 2;

    MiniOzoneHAClusterImpl cluster = getCluster();
    OzoneClient ozClient = OzoneClientFactory.getRpcClient(getConf());

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    ObjectStore store = ozClient.getObjectStore();

    // Create keys with all 3 OMs up.
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);

    Set<String> writtenKeys = new HashSet<>();
    for (int i = 1; i <= 50; i++) {
      String keyName = keyPrefix + i;
      writeTestData(store, volumeName, bucketName, keyName);
      writtenKeys.add(keyName);
    }

    // Make sure all OMs have logs from writing data, so we can check that
    // they are purged after prepare.
    for (OzoneManager om: cluster.getOzoneManagersList()) {
      LambdaTestUtils.await(timeoutMillis, 1000,
          () -> logFilesPresentInRatisPeer(om));
    }

    // Shut down one OM.
    cluster.stopOzoneManager(shutdownOMIndex);
    OzoneManager downedOM = cluster.getOzoneManager(shutdownOMIndex);
    Assert.assertFalse(downedOM.isRunning());

    // Write keys with the remaining OMs up.
    for (int i = 51; i <= 100; i++) {
      String keyName = keyPrefix + i;
      writeTestData(store, volumeName, bucketName, keyName);
      writtenKeys.add(keyName);
    }

    OzoneManagerProtocol ozoneManagerClient =
        ozClient.getObjectStore().getClientProxy().getOzoneManagerClient();
    long prepareIndex = ozoneManagerClient.prepareOzoneManager(
        PREPARE_FLUSH_WAIT_TIMEOUT_SECONDS, PREPARE_FLUSH_INTERVAL_SECONDS);

    // Check that the two live OMs are prepared.
    for (OzoneManager om: cluster.getOzoneManagersList()) {
      if (om != downedOM) {
        // Follower may still be applying transactions.
        waitAndCheckPrepared(om, prepareIndex);
      }
    }

    // Restart the downed OM and wait for it to catch up.
    // Since prepare was the last Ratis transaction, it should have all data
    // it missed once it receives the prepare transaction.
    cluster.restartOzoneManager(downedOM, true);
    LambdaTestUtils.await(timeoutMillis, 2000,
        () -> checkPrepared(downedOM, prepareIndex));

    // Make sure all OMs are prepared and still have data.
    for (OzoneManager om: cluster.getOzoneManagersList()) {
      List<OmKeyInfo> readKeys = om.getMetadataManager().listKeys(volumeName,
          bucketName, null, keyPrefix, 100);

      Assert.assertEquals(writtenKeys.size(), readKeys.size());
      for (OmKeyInfo keyInfo: readKeys) {
        Assert.assertTrue(writtenKeys.contains(keyInfo.getKeyName()));
      }
    }
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

  private void waitAndCheckPrepared(OzoneManager om,
      long prepareRequestLogIndex) throws Exception {
    // Log files are deleted after the snapshot is taken,
    // So once log files have been deleted, OM should be prepared.
    LambdaTestUtils.await(timeoutMillis, 1000,
        () -> !logFilesPresentInRatisPeer(om));
    Assert.assertTrue(checkPrepared(om, prepareRequestLogIndex));
  }

  private boolean checkPrepared(OzoneManager om, long prepareRequestLogIndex)
      throws Exception {
    // If no transactions have been persisted to the DB, transaction info
    // will be null, not zero.
    // This will cause a null pointer exception if we use
    // OzoneManager#getRatisSnapshotIndex to get the index from the DB.
    OMTransactionInfo txnInfo = om.getMetadataManager()
        .getTransactionInfoTable().get(TRANSACTION_INFO_KEY);
    return (txnInfo.getTransactionIndex() == prepareRequestLogIndex);
  }
}