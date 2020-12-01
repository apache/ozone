/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.RocksDBConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * This class sets up a MiniOzoneHACluster to test with Recon.
 */
public class TestReconWithOzoneManagerHA {
  @Rule
  public Timeout timeout = new Timeout(300_000);

  private MiniOzoneHAClusterImpl cluster;
  private ObjectStore objectStore;
  private final String omServiceId = "omService1";
  private final String volName = "testrecon";

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, Boolean.TRUE.toString());

    // Sync to disk enabled
    RocksDBConfiguration dbConf = conf.getObject(RocksDBConfiguration.class);
    dbConf.setSyncOption(true);
    conf.setFromObject(dbConf);

    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(UUID.randomUUID().toString())
        .setScmId(UUID.randomUUID().toString())
        .setOMServiceId(omServiceId)
        .setNumDatanodes(1)
        .setNumOfOzoneManagers(3)
        .includeRecon(true)
        .build();
    cluster.waitForClusterToBeReady();
    objectStore = OzoneClientFactory.getRpcClient(omServiceId, conf)
        .getObjectStore();
    objectStore.createVolume(volName);
    objectStore.getVolume(volName).createBucket(volName);
  }

  @After
  public void tearDown() {
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
    Assert.assertNotNull("Timed out waiting OM leader election to finish: "
        + "no leader or more than one leader.", ozoneManager);
    Assert.assertTrue("Should have gotten the leader!",
        ozoneManager.get().isLeaderReady());

    OzoneManagerServiceProviderImpl impl = (OzoneManagerServiceProviderImpl)
        cluster.getReconServer().getOzoneManagerServiceProvider();

    String hostname =
        ozoneManager.get().getHttpServer().getHttpAddress().getHostName();
    String expectedUrl = "http://" +
        (hostname.equals("0.0.0.0") ? "localhost" : hostname) + ":" +
        ozoneManager.get().getHttpServer().getHttpAddress().getPort() +
        OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT;
    String snapshotUrl = impl.getOzoneManagerSnapshotUrl();
    Assert.assertEquals("OM Snapshot should be requested from the leader.",
        expectedUrl, snapshotUrl);
    // Write some data
    String keyPrefix = "ratis";
    OzoneOutputStream key = objectStore.getVolume(volName)
        .getBucket(volName)
        .createKey(keyPrefix, 1024, ReplicationType.RATIS,
            ReplicationFactor.ONE, new HashMap<>());
    key.write(keyPrefix.getBytes());
    key.flush();
    key.close();
    // Sync data to Recon
    impl.syncDataFromOM();

    ContainerDBServiceProvider containerDBServiceProvider =
        cluster.getReconServer().getContainerDBServiceProvider();
    TableIterator iterator =
        containerDBServiceProvider.getContainerTableIterator();
    String reconKeyPrefix = null;
    while (iterator.hasNext()) {
      Table.KeyValue<ContainerKeyPrefix, Integer> keyValue =
          (Table.KeyValue<ContainerKeyPrefix, Integer>) iterator.next();
      reconKeyPrefix = keyValue.getKey().getKeyPrefix();
    }
    Assert.assertEquals("Container data should be synced to recon.",
        String.format("/%s/%s/%s", volName, volName, keyPrefix),
        reconKeyPrefix);
  }
}
