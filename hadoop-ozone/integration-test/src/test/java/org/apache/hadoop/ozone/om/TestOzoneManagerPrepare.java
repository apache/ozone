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
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.ratis.OMTransactionInfo;
import org.apache.hadoop.ozone.om.request.upgrade.OMPrepareRequest;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test OM prepare against actual mini cluster.
 */
public class TestOzoneManagerPrepare extends TestOzoneManagerHA {
  private static final String BUCKET = "bucket";
  private static final String VOLUME = "volume";
  private static final String KEY_PREFIX = "key";

  private static final int TIMEOUT_MILLIS = 120000;
  private final static long PREPARE_FLUSH_WAIT_TIMEOUT_SECONDS = 300L;
  private final static long PREPARE_FLUSH_INTERVAL_SECONDS = 5L;

  private MiniOzoneHAClusterImpl cluster;
  private ClientProtocol clientProtocol;
  private ObjectStore store;

  public void setup() throws Exception {
    cluster = getCluster();
    store = getObjectStore();
    clientProtocol = store.getClientProxy();
  }

  /**
   * Calls prepare on all OMs when they have no transaction information.
   * Checks that they are brought into prepare mode successfully.
   */
  @Test
  public void testPrepareWithoutTransactions() throws Exception {
    setup();
    long prepareIndex = submitPrepareRequest();
    assertClusterPrepared(prepareIndex);
  }

  /**
   * Writes data to the cluster via the leader OM, and then prepares it.
   * Checks that every OM is prepared successfully.
   */
  @Test
  public void testPrepareWithTransactions() throws Exception {
    setup();
    Set<String> writtenKeys = writeKeysAndWaitForLogs(50);
    long prepareIndex = submitPrepareRequest();

    // Make sure all OMs are prepared and all OMs still have their data.
    assertClusterPrepared(prepareIndex);
    assertKeysWritten(writtenKeys);
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
    setup();
    // Index of the OM that will be shut down during this test.
    final int shutdownOMIndex = 2;
    List<OzoneManager> runningOms = cluster.getOzoneManagersList();

    // Create keys with all 3 OMs up.
    Set<String> writtenKeys = writeKeysAndWaitForLogs(10, runningOms);

    // Shut down one OM.
    cluster.stopOzoneManager(shutdownOMIndex);
    OzoneManager downedOM = cluster.getOzoneManager(shutdownOMIndex);
    Assert.assertFalse(downedOM.isRunning());
    Assert.assertEquals(runningOms.remove(shutdownOMIndex), downedOM);

    // Write keys with the remaining OMs up.
    writtenKeys.addAll(
        writeKeysAndWaitForLogs(10, runningOms));

    long prepareIndex = submitPrepareRequest();

    // Check that the two live OMs are prepared.
    assertClusterPrepared(prepareIndex, runningOms);

    // Restart the downed OM and wait for it to catch up.
    // Since prepare was the last Ratis transaction, it should have all data
    // it missed once it receives the prepare transaction.
    cluster.restartOzoneManager(downedOM, true);
    runningOms.add(shutdownOMIndex, downedOM);

    // Make sure all OMs are prepared and still have data.
    assertClusterPrepared(prepareIndex, runningOms);
    assertKeysWritten(writtenKeys, runningOms);
  }

  @Test
  public void testPrepareWithRestart() throws Exception {
    setup();
    writeKeysAndWaitForLogs(10);
    long prepareIndex = submitPrepareRequest();
    assertClusterPrepared(prepareIndex);

    // Restart all ozone managers.
    for (OzoneManager om: cluster.getOzoneManagersList()) {
      cluster.shutdownOzoneManager(om);
      cluster.restartOzoneManager(om, true);
    }

    cluster.waitForClusterToBeReady();
    assertClusterPrepared(prepareIndex);
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

  private Set<String> writeKeysAndWaitForLogs(int numKeys) throws Exception {
    return writeKeysAndWaitForLogs(numKeys, cluster.getOzoneManagersList());
  }

  private Set<String> writeKeysAndWaitForLogs(int numKeys,
      List<OzoneManager> ozoneManagers) throws Exception {

    store.createVolume(VOLUME);
    OzoneVolume volume = store.getVolume(VOLUME);
    volume.createBucket(BUCKET);

    Set<String> writtenKeys = new HashSet<>();
    for (int i = 1; i <= numKeys; i++) {
      String keyName = KEY_PREFIX + i;
      writeTestData(VOLUME, BUCKET, keyName);
      writtenKeys.add(keyName);
    }

    // Make sure all OMs have logs from writing data, so we can check that
    // they are purged after prepare.
    for (OzoneManager om: ozoneManagers) {
      LambdaTestUtils.await(TIMEOUT_MILLIS, 1000,
          () -> logFilesPresentInRatisPeer(om));
    }

    return writtenKeys;
  }

  private void writeTestData(String volumeName,
      String bucketName, String keyName) throws Exception {

    String keyString = UUID.randomUUID().toString();
    byte[] data = ContainerTestHelper.getFixedLengthString(
        keyString, 100).getBytes(UTF_8);
    OzoneOutputStream keyStream = TestHelper.createKey(
        keyName, ReplicationType.STAND_ALONE, ReplicationFactor.ONE,
        100, store, volumeName, bucketName);
    keyStream.write(data);
    keyStream.close();
  }

  private void assertKeysWritten(Set<String> expectedKeys) throws Exception {
    assertKeysWritten(expectedKeys, cluster.getOzoneManagersList());
  }

  private void assertKeysWritten(Set<String> expectedKeys,
      List<OzoneManager> ozoneManagers) throws Exception {
    for (OzoneManager om: ozoneManagers) {
      List<OmKeyInfo> keys = om.getMetadataManager().listKeys(VOLUME,
          BUCKET, null, KEY_PREFIX, 100);

      Assert.assertEquals(expectedKeys.size(), keys.size());
      for (OmKeyInfo keyInfo: keys) {
        Assert.assertTrue(expectedKeys.contains(keyInfo.getKeyName()));
      }
    }
  }

  private long submitPrepareRequest() throws Exception {
    return clientProtocol.getOzoneManagerClient()
        .prepareOzoneManager(PREPARE_FLUSH_WAIT_TIMEOUT_SECONDS,
            PREPARE_FLUSH_INTERVAL_SECONDS);
  }

  private void assertClusterPrepared(long preparedIndex) throws Exception {
    assertClusterPrepared(preparedIndex, cluster.getOzoneManagersList());
  }

  private void assertClusterPrepared(long preparedIndex,
      List<OzoneManager> ozoneManagers) throws Exception {

    // Make sure the specified OMs are prepared individually.
    for (OzoneManager om : ozoneManagers) {
      waitAndAssertPrepared(om, preparedIndex);
    }

    // Submitting a read request should pass.
    clientProtocol.listVolumes(VOLUME, "", 100);

    // Submitting write request should fail.
    try {
      clientProtocol.createVolume("foo");
      Assert.fail("Write request should fail when OM is in prepare mode.");
    } catch (OMException ex) {
      Assert.assertEquals(OMException.ResultCodes.NOT_SUPPORTED_OPERATION,
          ex.getResult());
    }
  }

  private void waitAndAssertPrepared(OzoneManager om,
      long prepareRequestLogIndex) throws Exception {
    // Log files are deleted after the snapshot is taken,
    // So once log files have been deleted, OM should be prepared.
    LambdaTestUtils.await(TIMEOUT_MILLIS, 1000,
        () -> !logFilesPresentInRatisPeer(om));
    OMTransactionInfo txnInfo = om.getMetadataManager()
        .getTransactionInfoTable().get(TRANSACTION_INFO_KEY);
    Assert.assertEquals(txnInfo.getTransactionIndex(), prepareRequestLogIndex);
  }
}