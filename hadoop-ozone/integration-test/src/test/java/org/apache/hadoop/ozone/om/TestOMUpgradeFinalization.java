package org.apache.hadoop.ozone.om;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

import static org.apache.hadoop.ozone.OzoneConsts.LAYOUT_VERSION_KEY;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager.maxLayoutVersion;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse.PrepareStatus.PREPARE_COMPLETED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneOMHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.StatusAndMessages;

import org.apache.ozone.test.LambdaTestUtils;
import org.apache.ratis.util.LifeCycle;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for OM upgrade finalization.
 */
@RunWith(Parameterized.class)
public class TestOMUpgradeFinalization {

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = new Timeout(300000);
  private MiniOzoneOMHAClusterImpl cluster;
  private OzoneManager ozoneManager;
  private ClientProtocol clientProtocol;
  private int fromLayoutVersion;

  /**
   * Defines a "from" layout version to finalize from.
   *
   * @return
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {INITIAL_VERSION},
    });
  }


  public TestOMUpgradeFinalization(OMLayoutFeature fromVersion) {
    this.fromLayoutVersion = fromVersion.layoutVersion();
  }

  /**
   * Create a MiniDFSCluster for testing.
   */
  @Before
  public void setup() throws Exception {

    org.junit.Assume.assumeTrue("Check if there is need to finalize.",
        maxLayoutVersion() > fromLayoutVersion);

    OzoneConfiguration conf = new OzoneConfiguration();
    String omServiceId = UUID.randomUUID().toString();
    cluster = (MiniOzoneOMHAClusterImpl) MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(UUID.randomUUID().toString())
        .setScmId(UUID.randomUUID().toString())
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(3)
        .setNumDatanodes(1)
        .setOmLayoutVersion(fromLayoutVersion)
        .build();

    cluster.waitForClusterToBeReady();
    ozoneManager = cluster.getOzoneManager();
    ObjectStore objectStore = OzoneClientFactory.getRpcClient(omServiceId, conf)
        .getObjectStore();
    clientProtocol = objectStore.getClientProxy();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Currently this is a No-Op finalization since there is only one layout
   * version in OM. But this test is expected to remain consistent when a
   * new version is added.
   */
  @Test
  public void testOmFinalization() throws Exception {
    // Assert OM Layout Version is 'fromLayoutVersion' on deploy.
    assertEquals(fromLayoutVersion,
        ozoneManager.getVersionManager().getMetadataLayoutVersion());
    assertNull(ozoneManager.getMetadataManager()
        .getMetaTable().get(LAYOUT_VERSION_KEY));

    OzoneManagerProtocol omClient = clientProtocol.getOzoneManagerClient();
    StatusAndMessages response =
        omClient.finalizeUpgrade("finalize-test");
    System.out.println("Finalization Messages : " + response.msgs());

    waitForFinalization(omClient);

    LambdaTestUtils.await(30000, 3000, () -> {
      String lvString = ozoneManager.getMetadataManager().getMetaTable()
          .get(LAYOUT_VERSION_KEY);
      return maxLayoutVersion() == Integer.parseInt(lvString);
    });
  }

  @Test
  public void testOmFinalizationWithOneOmDown() throws Exception {

    List<OzoneManager> runningOms = cluster.getOzoneManagersList();
    final int shutdownOMIndex = 2;
    OzoneManager downedOM = cluster.getOzoneManager(shutdownOMIndex);
    cluster.stopOzoneManager(shutdownOMIndex);
    Assert.assertFalse(downedOM.isRunning());
    Assert.assertEquals(runningOms.remove(shutdownOMIndex), downedOM);

    OzoneManagerProtocol omClient = clientProtocol.getOzoneManagerClient();
    // Have to do a "prepare" operation to get rid of the logs in the active
    // OMs.
    long prepareIndex = omClient.prepareOzoneManager(120L, 5L);
    assertClusterPrepared(prepareIndex, runningOms);

    omClient.cancelOzoneManagerPrepare();
    StatusAndMessages response =
        omClient.finalizeUpgrade("finalize-test");
    System.out.println("Finalization Messages : " + response.msgs());

    waitForFinalization(omClient);
    cluster.restartOzoneManager(downedOM, true);

    try {
      waitFor(() -> downedOM.getOmRatisServer()
              .getOmStateMachine().getLifeCycleState().isPausingOrPaused(),
          1000, 60000);
    } catch (TimeoutException timeEx) {
      LifeCycle.State state = downedOM.getOmRatisServer()
          .getOmStateMachine().getLifeCycle().getCurrentState();
      if (state != LifeCycle.State.RUNNING) {
        Assert.fail("OM State Machine State expected to be in RUNNING state.");
      }
    }

    waitFor(() -> {
      LifeCycle.State lifeCycleState = downedOM.getOmRatisServer()
          .getOmStateMachine().getLifeCycle().getCurrentState();
      return !lifeCycleState.isPausingOrPaused();
    }, 1000, 60000);


    assertEquals(maxLayoutVersion(),
        ozoneManager.getVersionManager().getMetadataLayoutVersion());
    String lvString = ozoneManager.getMetadataManager().getMetaTable()
        .get(LAYOUT_VERSION_KEY);
    assertNotNull(lvString);
    assertEquals(maxLayoutVersion(),
        Integer.parseInt(lvString));
  }

  private void assertClusterPrepared(
      long preparedIndex, List<OzoneManager> ozoneManagers) throws Exception {
    for (OzoneManager om : ozoneManagers) {
      LambdaTestUtils.await(120000,
          1000, () -> {
            if (!om.isRunning()) {
              return false;
            } else {
              boolean preparedAtIndex = false;
              OzoneManagerPrepareState.State state =
                  om.getPrepareState().getState();

              if (state.getStatus() == PREPARE_COMPLETED) {
                if (state.getIndex() == preparedIndex) {
                  preparedAtIndex = true;
                } else {
                  // State will not change if we are prepared at the wrong
                  // index. Break out of wait.
                  throw new Exception("OM " + om.getOMNodeId() + " prepared " +
                      "but prepare index " + state.getIndex() + " does not " +
                      "match expected prepare index " + preparedIndex);
                }
              }
              return preparedAtIndex;
            }
          });
    }
  }

  private void waitForFinalization(OzoneManagerProtocol omClient)
      throws TimeoutException, InterruptedException {
    waitFor(() -> {
      try {
        StatusAndMessages statusAndMessages =
            omClient.queryUpgradeFinalizationProgress("finalize-test", false,
                false);
        System.out.println("Finalization Messages : " +
            statusAndMessages.msgs());
        return statusAndMessages.status().equals(FINALIZATION_DONE);
      } catch (IOException e) {
        Assert.fail(e.getMessage());
      }
      return false;
    }, 2000, 20000);
  }

}
