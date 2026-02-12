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

package org.apache.hadoop.ozone.recon;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_HTTP_ENDPOINT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.RocksDBConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.ReconContainerMetadataManagerImpl;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskControllerImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class sets up a MiniOzoneOMHACluster to test with Recon.
 */
public class TestReconWithOzoneManagerHA {

  private MiniOzoneHAClusterImpl cluster;
  private ObjectStore objectStore;
  private static final String OM_SERVICE_ID = "omService1";
  private static final String VOL_NAME = "testrecon";
  private OzoneClient client;
  private ReconService recon;
  private TestReconOmMetaManagerUtils omMetaManagerUtils = new TestReconOmMetaManagerUtils();

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    // Sync to disk enabled
    RocksDBConfiguration dbConf = conf.getObject(RocksDBConfiguration.class);
    dbConf.setSyncOption(true);
    conf.setFromObject(dbConf);

    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf);
    recon = new ReconService(conf);
    builder.setOMServiceId(OM_SERVICE_ID)
        .setNumOfOzoneManagers(3)
        .setNumDatanodes(1)
        .addService(recon);
    cluster = builder.build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, conf);
    objectStore = client.getObjectStore();
    objectStore.createVolume(VOL_NAME);
    // TODO: HDDS-5463
    //  Recon's container ID to key mapping does not yet support FSO buckets.
    objectStore.getVolume(VOL_NAME).createBucket(VOL_NAME,
        BucketArgs.newBuilder().setBucketLayout(BucketLayout.OBJECT_STORE)
            .build());
  }

  @AfterEach
  public void tearDown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testReconGetsSnapshotFromLeader() throws Exception {
    AtomicReference<OzoneManager> ozoneManager = new AtomicReference<>();
    // Wait for OM leader election to finish
    GenericTestUtils.waitFor(() -> {
      OzoneManager om = cluster.getOMLeader();
      ozoneManager.set(om);
      return om != null;
    }, 100, 120000);
    assertNotNull(ozoneManager, "Timed out waiting OM leader election to finish: "
        + "no leader or more than one leader.");
    assertTrue(ozoneManager.get().isLeaderReady(), "Should have gotten the leader!");

    OzoneManagerServiceProviderImpl impl = (OzoneManagerServiceProviderImpl)
        recon.getReconServer().getOzoneManagerServiceProvider();

    String hostname =
        ozoneManager.get().getHttpServer().getHttpAddress().getHostName();
    String expectedUrl = "http://" +
        (hostname.equals("0.0.0.0") ? "localhost" : hostname) + ":" +
        ozoneManager.get().getHttpServer().getHttpAddress().getPort() +
        OZONE_DB_CHECKPOINT_HTTP_ENDPOINT;
    String snapshotUrl = impl.getOzoneManagerSnapshotUrl();
    assertEquals(expectedUrl, snapshotUrl);
    // Write some data
    String keyPrefix = "ratis";
    OzoneOutputStream key = objectStore.getVolume(VOL_NAME)
        .getBucket(VOL_NAME)
        .createKey(keyPrefix, 1024, ReplicationType.RATIS,
            ReplicationFactor.ONE, new HashMap<>());
    key.write(keyPrefix.getBytes(UTF_8));
    key.flush();
    key.close();
    // Sync data to Recon
    impl.syncDataFromOM();

    // Wait for async event processing to complete
    // Events are processed asynchronously, so wait for processing to finish
    ReconTaskControllerImpl reconTaskController =
        (ReconTaskControllerImpl) recon.getReconServer().getReconTaskController();
    CompletableFuture<Void> completableFuture =
        omMetaManagerUtils.waitForEventBufferEmpty(reconTaskController.getEventBuffer());
    GenericTestUtils.waitFor(completableFuture::isDone, 100, 30000);

    final ReconContainerMetadataManagerImpl reconContainerMetadataManager =
        (ReconContainerMetadataManagerImpl) recon.getReconServer().getReconContainerMetadataManager();
    try (Table.KeyValueIterator<ContainerKeyPrefix, Integer> iterator
        = reconContainerMetadataManager.getContainerKeyTableForTesting().iterator()) {
      String reconKeyPrefix = null;
      while (iterator.hasNext()) {
        reconKeyPrefix = iterator.next().getKey().getKeyPrefix();
      }
      assertEquals(
          String.format("/%s/%s/%s", VOL_NAME, VOL_NAME, keyPrefix),
          reconKeyPrefix);
    }
  }
}
