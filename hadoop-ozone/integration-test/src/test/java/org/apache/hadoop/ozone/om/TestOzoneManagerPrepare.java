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
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION_WHEN_PREPARED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus;
import org.apache.ozone.test.LambdaTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ozone.test.tag.Slow;
import org.apache.ozone.test.tag.Unhealthy;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test OM prepare against actual mini cluster.
 */
@Flaky("HDDS-5990")
public class TestOzoneManagerPrepare extends TestOzoneManagerHA {
  private static final String BUCKET = "bucket";
  private static final String VOLUME = "volume";
  private static final String KEY_PREFIX = "key";

  // Maximum time to wait for conditions involving Ratis logs.
  private static final int WAIT_TIMEOUT_MILLIS = 120000;
  private static final long PREPARE_FLUSH_WAIT_TIMEOUT_SECONDS = 120L;
  private static final long PREPARE_FLUSH_INTERVAL_SECONDS = 5L;

  private MiniOzoneHAClusterImpl cluster;
  private ClientProtocol clientProtocol;
  private ObjectStore store;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneManagerPrepare.class);

  private void initInstanceVariables() {
    cluster = getCluster();
    store = getObjectStore();
    clientProtocol = store.getClientProxy();
  }

  /**
   * Make sure OM is out of Prepare state before executing individual tests.
   */
  @BeforeEach
  void setup() throws Exception {
    initInstanceVariables();

    LOG.info("Waiting for OM leader election");
    waitForLeaderToBeReady();
    submitCancelPrepareRequest();
    assertClusterNotPrepared();
  }

  /**
   * Reset cluster between tests.
   */
  @AfterEach
  void resetCluster() throws Exception {
    if (cluster != null) {
      cluster.restartOzoneManager();
    }
  }

  /**
   * Writes data to the cluster via the leader OM, and then prepares it.
   * Checks that every OM is prepared successfully.
   */
  @Test
  public void testPrepareWithTransactions() throws Exception {
    long prepareIndex = submitPrepareRequest();
    assertClusterPrepared(prepareIndex);
    assertRatisLogsCleared();

    submitCancelPrepareRequest();
    assertClusterNotPrepared();

    String volumeName = VOLUME + UUID.randomUUID().toString();
    Set<String> writtenKeys = writeKeysAndWaitForLogs(volumeName, 50,
        cluster.getOzoneManagersList());
    prepareIndex = submitPrepareRequest();

    // Make sure all OMs are prepared and all OMs still have their data.
    assertClusterPrepared(prepareIndex);
    assertRatisLogsCleared();
    assertKeysWritten(volumeName, writtenKeys);

    // Should be able to "prepare" the OM group again.
    assertShouldBeAbleToPrepare();
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
  @Unhealthy("RATIS-1481") // until upgrade to Ratis 2.3.0
  public void testPrepareDownedOM() throws Exception {
    // Index of the OM that will be shut down during this test.
    final int shutdownOMIndex = 2;
    List<OzoneManager> runningOms = cluster.getOzoneManagersList();

    String volumeName1 = VOLUME + UUID.randomUUID().toString();
    // Create keys with all 3 OMs up.
    Set<String> writtenKeysBeforeOmShutDown = writeKeysAndWaitForLogs(
        volumeName1, 10, runningOms);

    // Shut down one OM.
    cluster.stopOzoneManager(shutdownOMIndex);
    OzoneManager downedOM = cluster.getOzoneManager(shutdownOMIndex);
    assertFalse(downedOM.isRunning());
    assertEquals(runningOms.remove(shutdownOMIndex), downedOM);

    // Write keys with the remaining OMs up.
    String volumeName2 = VOLUME + UUID.randomUUID().toString();
    Set<String> writtenKeysAfterOmShutDown =
        writeKeysAndWaitForLogs(volumeName2, 10, runningOms);

    long prepareIndex = submitPrepareRequest();

    // Check that the two live OMs are prepared.
    assertClusterPrepared(prepareIndex, runningOms);

    // Restart the downed OM and wait for it to catch up.
    // Since prepare was the last Ratis transaction, it should have all data
    // it missed once it receives the prepare transaction.
    cluster.restartOzoneManager(downedOM, true);
    runningOms.add(shutdownOMIndex, downedOM);
    ExitUtils.assertNotTerminated();

    // Make sure all OMs are prepared and still have data.
    assertClusterPrepared(prepareIndex, runningOms);
    assertKeysWritten(volumeName1, writtenKeysBeforeOmShutDown, runningOms);
    assertKeysWritten(volumeName2, writtenKeysAfterOmShutDown, runningOms);

    // Cancelling prepare state of the cluster to try out an operation.
    submitCancelPrepareRequest();
    assertClusterNotPrepared();

    // Should be able to write data to all 3 OMs.
    String volumeName3 = VOLUME + UUID.randomUUID().toString();
    store.createVolume(volumeName3);
    for (OzoneManager om : runningOms) {
      LambdaTestUtils.await(WAIT_TIMEOUT_MILLIS, 1000, () -> {
        OMMetadataManager metadataManager = om.getMetadataManager();
        String volumeKey = metadataManager.getVolumeKey(volumeName3);
        return metadataManager.getVolumeTable().get(volumeKey) != null;
      });
    }
  }

  @Test
  public void testPrepareWithRestart() throws Exception {
    // Create fresh cluster for this test to prevent timeout from restarting
    // modified cluster.
    shutdown();
    init();
    initInstanceVariables();

    String volumeName = VOLUME + UUID.randomUUID().toString();
    writeKeysAndWaitForLogs(volumeName, 10);

    long prepareIndex = submitPrepareRequest();
    assertClusterPrepared(prepareIndex);

    // Restart all ozone managers.
    cluster.restartOzoneManager();

    // No check for cleared logs, since Ratis meta transactions may slip in
    // on restart.
    assertClusterPrepared(prepareIndex);
  }

  @Slow("Saving on CI time since this is a pessimistic test. We should not " +
      "be able to do anything with 2 OMs down.")
  @Test
  public void testPrepareFailsWhenTwoOmsAreDown() throws Exception {
    // Shut down 2 OMs.
    for (int i : Arrays.asList(1, 2)) {
      cluster.stopOzoneManager(i);
      OzoneManager downedOM = cluster.getOzoneManager(i);
      assertFalse(downedOM.isRunning());
    }

    assertThrows(IOException.class,
        () -> clientProtocol.getOzoneManagerClient().prepareOzoneManager(
            PREPARE_FLUSH_WAIT_TIMEOUT_SECONDS,
            PREPARE_FLUSH_INTERVAL_SECONDS));
  }

  /**
   * Issues requests on ten different threads, for which one is a prepare and
   * the rest are create volume. We cannot be sure of the exact order that
   * the requests will execute, so this test checks that the cluster ends in
   * a prepared state, and that create volume requests either succeed, or fail
   * indicating the cluster was prepared before they were encountered.
   *
   * @throws Exception
   */
  @Test
  public void testPrepareWithMultipleThreads() throws Exception {
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
          assertTrue(clientProtocol.listVolumes(volumeName, "", 1)
              .stream()
              .anyMatch((vol) -> vol.getName().equals(volumeName)));
        } catch (ExecutionException ex) {
          OMException cause = assertInstanceOf(OMException.class, ex.getCause());
          assertEquals(NOT_SUPPORTED_OPERATION_WHEN_PREPARED, cause.getResult());
        }
      }
    }

    // In the above loop, we have waited for all threads to terminate.
    executorService.shutdown();
  }

  @Test
  public void testCancelPrepare() throws Exception {
    String volumeName = VOLUME + UUID.randomUUID().toString();
    Set<String> writtenKeys = writeKeysAndWaitForLogs(volumeName, 10);
    long prepareIndex = submitPrepareRequest();

    // Make sure all OMs are prepared and all OMs still have their data.
    assertClusterPrepared(prepareIndex);
    assertRatisLogsCleared();
    assertKeysWritten(volumeName, writtenKeys);

    // Cancel prepare and check that data is still present.
    submitCancelPrepareRequest();
    assertClusterNotPrepared();
    assertKeysWritten(volumeName, writtenKeys);

    // Cancelling prepare again should have no effect.
    submitCancelPrepareRequest();
    assertClusterNotPrepared();

    // Write more data after cancelling prepare.
    String volumeNameNew = VOLUME + UUID.randomUUID().toString();
    writtenKeys = writeKeysAndWaitForLogs(volumeNameNew, 10);

    // Cancelling prepare again should have no effect and new data should be
    // preserved.
    submitCancelPrepareRequest();
    assertClusterNotPrepared();
    assertKeysWritten(volumeNameNew, writtenKeys);
  }

  private boolean logFilesPresentInRatisPeer(OzoneManager om) {
    final RaftServer.Division server = om.getOmRatisServer().getServerDivision();
    final String ratisDir = server.getRaftServer().getProperties().get("raft.server.storage.dir");
    final String groupIdDirName = server.getGroup().getGroupId().getUuid().toString();
    File logDir = Paths.get(ratisDir, groupIdDirName, "current")
        .toFile();

    File[] files = logDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.getName().startsWith("log")) {
          return true;
        }
      }
    }
    return false;
  }

  private Set<String> writeKeysAndWaitForLogs(String volumeName,
                                              int numKeys) throws Exception {
    return writeKeysAndWaitForLogs(volumeName, numKeys,
        cluster.getOzoneManagersList());
  }

  private Set<String> writeKeysAndWaitForLogs(String volumeName, int numKeys,
      List<OzoneManager> ozoneManagers) throws Exception {

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(BUCKET);

    Set<String> writtenKeys = new HashSet<>();
    for (int i = 1; i <= numKeys; i++) {
      String keyName = KEY_PREFIX + i;
      writeTestData(volumeName, BUCKET, keyName);
      writtenKeys.add(keyName);
    }

    // Make sure all OMs have logs from writing data, so we can check that
    // they are purged after prepare.
    for (OzoneManager om : ozoneManagers) {
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
    TestDataUtil.createKey(store.getVolume(volumeName).
        getBucket(bucketName), keyName, data);
  }

  private void assertKeysWritten(String volumeName,
                                 Set<String> expectedKeys) throws Exception {
    assertKeysWritten(volumeName, expectedKeys, cluster.getOzoneManagersList());
  }

  /**
   * Checks that all provided OMs have {@code expectedKeys} in the volume
   * {@code volumeName} and retries checking until the test timeout.
   * All provided OMs are checked, not just a majority, so that we can
   * test that downed OMs are able to make a full recovery after preparation,
   * even though the cluster could appear healthy with just 2 OMs.
   */
  private void assertKeysWritten(String volumeName, Set<String> expectedKeys,
      List<OzoneManager> ozoneManagers) throws Exception {
    for (OzoneManager om: ozoneManagers) {
      // Wait for a potentially slow follower to apply all key writes.
      LambdaTestUtils.await(WAIT_TIMEOUT_MILLIS, 1000, () -> {
        List<OmKeyInfo> keys = om.getMetadataManager().listKeys(volumeName,
            BUCKET, null, KEY_PREFIX, 100).getKeys();

        boolean allKeysFound = (expectedKeys.size() == keys.size());
        if (!allKeysFound) {
          LOG.info("In {} waiting for number of keys {} to equal " +
              "expected number of keys {}.", om.getOMNodeId(),
              keys.size(), expectedKeys.size());
        } else {
          for (OmKeyInfo keyInfo : keys) {
            if (!expectedKeys.contains(keyInfo.getKeyName())) {
              allKeysFound = false;
              LOG.info("In {} expected keys did not contain key {}",
                  om.getOMNodeId(), keyInfo.getKeyName());
              break;
            }
          }
        }

        return allKeysFound;
      });
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

  private void assertClusterPrepared(long expectedPreparedIndex)
      throws Exception {
    assertClusterPrepared(expectedPreparedIndex,
        cluster.getOzoneManagersList());
  }

  private void assertClusterPrepared(long expectedPreparedIndex,
      List<OzoneManager> ozoneManagers) throws Exception {

    for (OzoneManager om : ozoneManagers) {
      // Wait for each OM to be running and transaction info to match to know
      // it is prepared.
      LambdaTestUtils.await(WAIT_TIMEOUT_MILLIS,
          1000, () -> {
          if (!om.isRunning()) {
            LOG.info("{} is not yet started.", om.getOMNodeId());
            return false;
          } else {
            OzoneManagerPrepareState.State state =
                om.getPrepareState().getState();

            LOG.info("{} has prepare status: {} prepare index: {}.",
                om.getOMNodeId(), state.getStatus(), state.getIndex());

            return (state.getStatus() == PrepareStatus.PREPARE_COMPLETED) &&
                (state.getIndex() >= expectedPreparedIndex);
          }
        });
    }

    // Submitting a read request should pass.
    clientProtocol.listVolumes(VOLUME, "", 100);

    // Submitting write request should fail.
    OMException omException = assertThrows(OMException.class,
        () -> clientProtocol.createVolume("vol"));
    assertEquals(NOT_SUPPORTED_OPERATION_WHEN_PREPARED,
        omException.getResult());
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
                  PrepareStatus.NOT_PREPARED;
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

  private void assertShouldBeAbleToPrepare() throws Exception {
    long prepareIndex = submitPrepareRequest();
    assertClusterPrepared(prepareIndex);
    assertRatisLogsCleared();
  }
}
