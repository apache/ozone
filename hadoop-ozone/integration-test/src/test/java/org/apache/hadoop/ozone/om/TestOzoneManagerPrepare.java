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

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test OM prepare against actual mini cluster.
 */
public class TestOzoneManagerPrepare extends TestOzoneManagerHA {
  private static final String BUCKET = "bucket";
  private static final String VOLUME = "volume";
  private static final String KEY_PREFIX = "key";

  // Maximum time to wait for conditions involving Ratis logs.
  private static final int WAIT_TIMEOUT_MILLIS = 120000;
  private final static long PREPARE_FLUSH_WAIT_TIMEOUT_SECONDS = 120L;
  private final static long PREPARE_FLUSH_INTERVAL_SECONDS = 5L;

  private MiniOzoneHAClusterImpl cluster;
  private ClientProtocol clientProtocol;
  private ObjectStore store;

  public void setup() throws Exception {
    cluster = getCluster();
    store = getObjectStore();
    clientProtocol = store.getClientProxy();

    store.createVolume(VOLUME);
    OzoneVolume volume = store.getVolume(VOLUME);
    volume.createBucket(BUCKET);
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
    assertRatisLogsCleared();
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
    assertRatisLogsCleared();
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
  // TODO: This test should be passing after HDDS-4610 and RATIS-1241
  // @Test
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

  // TODO: This test should be passing after HDDS-4610 and RATIS-1241
  // @Test
  public void testPrepareWithRestart() throws Exception {
    setup();
    writeKeysAndWaitForLogs(10);
    long prepareIndex = submitPrepareRequest();
    assertClusterPrepared(prepareIndex);

    // Restart all ozone managers.
    cluster.restartOzoneManager();

    // No check for cleared logs, since Ratis meta transactions may slip in
    // on restart.
    assertClusterPrepared(prepareIndex);
  }

  /**
   * Issues requests on ten different threads, for which one is a prepare and
   * the rest are create volume. We cannot be sure of the exact order that
   * the requests will execute, so this test checks that the cluster ends in
   * a prepared state, and that create volume requests either succeed, or fail
   * indicating the cluster was prepared before they were encountered.
   * @throws Exception
   */
  @Test
  public void testPrepareWithMultipleThreads() throws Exception {
    setup();
    final int numThreads = 10;
    final int prepareTaskIndex = 5;

    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    // For the prepare task, the future will return a log index.
    // For the create volume tasks, 0 (dummy value) will be returned.
    List<Future<Long>> tasks = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      Callable<Long> task;
      if (i == prepareTaskIndex) {
        task = this::submitPrepareRequest;
      } else {
        String volumeName = VOLUME + i;
        task = () -> {
          clientProtocol.createVolume(volumeName);
          return 0L;
        };
      }
      tasks.add(executorService.submit(task));
    }

    // For each task, wait for it to complete and check its result.
    for (int i = 0; i < numThreads; i++) {
      Future<Long> future = tasks.get(i);

      if (i == prepareTaskIndex) {
        assertClusterPrepared(future.get());
        assertRatisLogsCleared();
      } else {
        try {
          // If this throws an exception, it should be an OMException
          // indicating failure because the cluster was already prepared.
          // If no exception is thrown, the volume should be created.
          future.get();
          String volumeName = VOLUME + i;
          Assert.assertTrue(clientProtocol.listVolumes(volumeName, "", 1)
              .stream()
              .anyMatch((vol) -> vol.getName().equals(volumeName)));
        } catch (ExecutionException ex) {
          Throwable cause = ex.getCause();
          Assert.assertTrue(cause instanceof OMException);
          Assert.assertEquals(
              OMException.ResultCodes.NOT_SUPPORTED_OPERATION_WHEN_PREPARED,
              ((OMException) cause).getResult());
        }
      }
    }

    // In the above loop, we have waited for all threads to terminate.
    executorService.shutdown();
  }

  @Test
  public void testCancelPrepare() throws Exception {
    setup();
    Set<String> writtenKeys = writeKeysAndWaitForLogs(10);
    long prepareIndex = submitPrepareRequest();

    // Make sure all OMs are prepared and all OMs still have their data.
    assertClusterPrepared(prepareIndex);
    assertRatisLogsCleared();
    assertKeysWritten(writtenKeys);

    // Cancel prepare and check that data is still present.
    submitCancelPrepareRequest();
    assertClusterNotPrepared();
    assertKeysWritten(writtenKeys);

    // Cancelling prepare again should have no effect.
    submitCancelPrepareRequest();
    assertClusterNotPrepared();

    // Write more data after cancelling prepare.
    writtenKeys.addAll(writeKeysAndWaitForLogs(10));

    // Cancelling prepare again should have no effect and new data should be
    // preserved.
    submitCancelPrepareRequest();
    assertClusterNotPrepared();
    assertKeysWritten(writtenKeys);
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

    Set<String> writtenKeys = new HashSet<>();
    for (int i = 1; i <= numKeys; i++) {
      String keyName = KEY_PREFIX + i;
      writeTestData(VOLUME, BUCKET, keyName);
      writtenKeys.add(keyName);
    }

    // Make sure all OMs have logs from writing data, so we can check that
    // they are purged after prepare.
    for (OzoneManager om: ozoneManagers) {
      LambdaTestUtils.await(WAIT_TIMEOUT_MILLIS, 1000,
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

  private void submitCancelPrepareRequest() throws Exception {
    clientProtocol.getOzoneManagerClient().cancelOzoneManagerPrepare();
  }

  private void assertClusterPrepared(long preparedIndex) throws Exception {
    assertClusterPrepared(preparedIndex, cluster.getOzoneManagersList());
  }

  private void assertClusterPrepared(long preparedIndex,
      List<OzoneManager> ozoneManagers) throws Exception {

    for (OzoneManager om : ozoneManagers) {
      // Wait for each OM to be running and transaction info to match to know
      // it is prepared.
      LambdaTestUtils.await(WAIT_TIMEOUT_MILLIS,
          1000, () -> {
          if (!om.isRunning()) {
            return false;
          } else {
            boolean preparedAtIndex = false;
            OzoneManagerPrepareState.State state =
                om.getPrepareState().getState();

            if (state.getStatus() == PrepareStatus.PREPARE_COMPLETED) {
              if (state.getIndex() == preparedIndex) {
                preparedAtIndex = true;
              } else {
                // State will not change if we are prepared at the wrong index.
                // Break out of wait.
                throw new Exception("OM " + om.getOMNodeId() + " prepared " +
                    "but prepare index " + state.getIndex() + " does not " +
                    "match expected prepare index " + preparedIndex);
              }
            }
            return preparedAtIndex;
          }
        });
    }

    // Submitting a read request should pass.
    clientProtocol.listVolumes(VOLUME, "", 100);

    // Submitting write request should fail.
    try {
      clientProtocol.createVolume("vol");
      Assert.fail("Write request should fail when OM is in prepare mode.");
    } catch (OMException ex) {
      Assert.assertEquals(
          OMException.ResultCodes.NOT_SUPPORTED_OPERATION_WHEN_PREPARED,
          ex.getResult());
    }
  }

  private void assertClusterNotPrepared() throws Exception {
    assertClusterNotPrepared(cluster.getOzoneManagersList());
  }

  private void assertClusterNotPrepared(List<OzoneManager> ozoneManagers)
      throws Exception {
    for (OzoneManager om : ozoneManagers) {
      LambdaTestUtils.await(WAIT_TIMEOUT_MILLIS,
          1000, () -> {
            if (!om.isRunning()) {
              return false;
            } else {
              return om.getPrepareState().getState().getStatus() ==
                  PrepareStatus.PREPARE_NOT_STARTED;
            }
          });
    }

    // Submitting a read request should pass.
    clientProtocol.listVolumes(VOLUME, "", 100);

    // Submitting write request should also pass.
    clientProtocol.createVolume("vol");
    clientProtocol.deleteVolume("vol");
  }

  private void assertRatisLogsCleared() throws Exception {
    assertRatisLogsCleared(cluster.getOzoneManagersList());
  }

  private void assertRatisLogsCleared(List<OzoneManager> ozoneManagers)
      throws Exception {
    for (OzoneManager om: ozoneManagers) {
      LambdaTestUtils.await(WAIT_TIMEOUT_MILLIS, 1000,
          () -> !logFilesPresentInRatisPeer(om));
    }
  }
}