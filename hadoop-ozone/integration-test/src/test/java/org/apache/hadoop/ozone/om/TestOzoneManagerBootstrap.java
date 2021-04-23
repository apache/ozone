/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import com.google.common.base.Supplier;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import static org.apache.hadoop.ozone.OzoneConsts.SCM_DUMMY_SERVICE_ID;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.TestOzoneManagerHA.createKey;

public class TestOzoneManagerBootstrap {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public Timeout timeout = new Timeout(500_000);

  private MiniOzoneHAClusterImpl cluster = null;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private final String clusterId = UUID.randomUUID().toString();
  private final String scmId = UUID.randomUUID().toString();

  private static final int NUM_INITIAL_OMS = 3;

  private static final String OM_SERVICE_ID = "om-bootstrap";
  private static final String VOLUME_NAME;
  private static final String BUCKET_NAME;

  private long lastTransactionIndex;

  static {
    VOLUME_NAME = "volume" + RandomStringUtils.randomNumeric(5);
    BUCKET_NAME = "bucket" + RandomStringUtils.randomNumeric(5);
  }

  private void setupCluster() throws Exception {
    setupCluster(NUM_INITIAL_OMS);
  }

  private void setupCluster(int numInitialOMs) throws Exception {
    conf = new OzoneConfiguration();
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setSCMServiceId(SCM_DUMMY_SERVICE_ID)
        .setOMServiceId(OM_SERVICE_ID)
        .setNumOfOzoneManagers(numInitialOMs)
        .build();
    cluster.waitForClusterToBeReady();
    objectStore = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, conf)
        .getObjectStore();

    // Perform some transactions
    objectStore.createVolume(VOLUME_NAME);
    OzoneVolume volume = objectStore.getVolume(VOLUME_NAME);
    volume.createBucket(BUCKET_NAME);
    OzoneBucket bucket = volume.getBucket(BUCKET_NAME);
    createKey(bucket);

    lastTransactionIndex = cluster.getOMLeader().getRatisSnapshotIndex();
  }

  @After
  public void shutdown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void assertNewOMExistsInPeerList(String nodeId) throws Exception {
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      Assert.assertTrue("New OM node " + nodeId + " not present in Peer list " +
              "of OM " + om.getOMNodeId(), om.doesPeerExist(nodeId));
    }
    OzoneManager newOM = cluster.getOzoneManager(nodeId);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          return newOM.getRatisSnapshotIndex() >= lastTransactionIndex;
        } catch (IOException e) {
          return false;
        }
      }
    }, 100, 10000);

    // Check Ratis Dir for log files
    File[] logFiles = getRatisLogFiles(newOM);
    Assert.assertTrue("There are no ratis logs in new OM ",
        logFiles.length > 0);
  }

  private File[] getRatisLogFiles(OzoneManager om) {
    OzoneManagerRatisServer newOMRatisServer = om.getOmRatisServer();
    File ratisDir = new File(newOMRatisServer.getRatisStorageDir(),
        newOMRatisServer.getRaftGroupId().getUuid().toString());
    File ratisLogDir = new File(ratisDir, Storage.STORAGE_DIR_CURRENT);
    return ratisLogDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().startsWith("log");
      }
    });
  }

  private List<String> testBootstrapOMs(int numNewOMs) throws Exception {
    List<String> newOMNodeIds = new ArrayList<>(numNewOMs);
    for (int i = 1; i <= numNewOMs; i++) {
      String nodeId =  "omNode-bootstrap-" + i;
      cluster.bootstrapOzoneManager(nodeId);
      assertNewOMExistsInPeerList(nodeId);
      newOMNodeIds.add(nodeId);
    }
    return newOMNodeIds;
  }

  /**
   * Add 1 new OM to cluster.
   * @throws Exception
   */
  @Test
  public void testBootstrapOneNewOM() throws Exception {
    setupCluster();
    testBootstrapOMs(1);
  }

  /**
   * Add 2 new OMs to cluster.
   * @throws Exception
   */
  @Test
  public void testBootstrapTwoNewOMs() throws Exception {
    setupCluster();
    testBootstrapOMs(2);
  }

  /**
   * Add 2 new OMs to a 1 node OM cluster. Verify that one of the new OMs
   * must becomes the leader by stopping the old OM.
   */
  @Test
  public void testLeaderChangeToNewOM() throws Exception {
    setupCluster(1);
    OzoneManager oldOM = cluster.getOzoneManager();
    List<String> newOMNodeIds = testBootstrapOMs(2);

    // Stop old OM
    cluster.stopOzoneManager(oldOM.getOMNodeId());

    // Wait for Leader Election timeout
    Thread.sleep(OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT
        .toLong(TimeUnit.MILLISECONDS) * 3);

    // Verify that one of the new OMs is the leader
    GenericTestUtils.waitFor(() -> cluster.getOMLeader() != null, 500, 30000);
    OzoneManager omLeader = cluster.getOMLeader();

    Assert.assertTrue("New Bootstrapped OM not elected Leader even though " +
        "other OMs are down", newOMNodeIds.contains(omLeader.getOMNodeId()));

    // Perform some read and write operations with new OM leader
    objectStore = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, conf)
        .getObjectStore();
    OzoneVolume volume = objectStore.getVolume(VOLUME_NAME);
    OzoneBucket bucket = volume.getBucket(BUCKET_NAME);
    String key = createKey(bucket);

    Assert.assertNotNull(bucket.getKey(key));
  }
}
