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

package org.apache.hadoop.ozone.client.rpc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.ozone.HddsDatanodeService.TESTING_DATANODE_VERSION_CURRENT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.OzoneTestHelper;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * End-to-end test that a datanode's reported {@code currentVersion} flows all
 * the way from the datanode to the client, on both the write and read paths.
 * <p>
 * A single {@link MiniOzoneCluster} is shared across all parameter values and is
 * never restarted. To exercise a given version, the test reloads the
 * {@link HddsDatanodeService#TESTING_DATANODE_VERSION_CURRENT} config on every
 * live datanode. The heartbeat task re-reads that config on every heartbeat, so
 * the datanodes start advertising the new version to SCM without a restart; once
 * they all report the same version SCM converges on it and stays there (every
 * heartbeat now carries it), so the client observations below are stable.
 * <p>
 * The version is then checked on three client paths:
 * <ul>
 *   <li><b>write path</b> — the pipeline SCM hands back when a block is
 *   allocated for a new key. SCM forwards the pipeline-wide <em>minimum</em>
 *   currentVersion; with every datanode at the same version the minimum equals
 *   that version.</li>
 *   <li><b>closed-container read</b> — looking up a key whose container is
 *   closed. SCM builds the read pipeline from the container replicas, which
 *   carry each datanode's own currentVersion.</li>
 *   <li><b>open-container read</b> — looking up a key whose container is still
 *   open. SCM returns the open pipeline but refreshes each member's
 *   currentVersion from the live node registry, so it too reflects the current
 *   per-datanode version.</li>
 * </ul>
 * <p>
 * All three paths are exercised for both RATIS (factor three) and EC (rs-3-2)
 * replication, since the two use different pipeline-provider code paths.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestDatanodeCurrentVersionEndToEnd {

  private static final int PROPAGATION_TIMEOUT_MS = 30_000;
  private static final ReplicationConfig RATIS_THREE =
      RatisReplicationConfig.getInstance(ReplicationFactor.THREE);
  private static final ReplicationConfig EC_3_2 = new ECReplicationConfig(3, 2);

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private StorageContainerManager scm;
  private OzoneBucket bucket;
  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME = "bucket1";
  private static final String RATIS_CLOSED_KEY = "closed-key";
  private static final String RATIS_OPEN_KEY = "open-key";
  private static final String EC_CLOSED_KEY = "ec-closed-key";
  private static final String EC_OPEN_KEY = "ec-open-key";

  @BeforeAll
  void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    // Fast heartbeats so a reloaded version reaches SCM quickly and the closed
    // container converges quickly during setup.
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    // Five datanodes so an EC rs-3-2 pipeline (3 data + 2 parity) has enough
    // members; RATIS/THREE simply uses three of them.
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    client = cluster.newClient();
    ObjectStore store = client.getObjectStore();
    store.createVolume(VOLUME_NAME);
    store.getVolume(VOLUME_NAME).createBucket(BUCKET_NAME);
    bucket = store.getVolume(VOLUME_NAME).getBucket(BUCKET_NAME);

    // Pre-create keys whose containers are closed so that read-path lookups
    // resolve the per-datanode read pipeline (built over the closed replicas),
    // which reflects each datanode's live currentVersion. Also pre-create keys
    // whose containers stay OPEN; open-container reads resolve the live open
    // pipeline, which must reflect current datanode versions too. Cover both
    // RATIS and EC replication.
    byte[] data = "current-version".getBytes(UTF_8);
    createClosedContainerKey(RATIS_CLOSED_KEY, RATIS_THREE, data);
    createClosedContainerKey(EC_CLOSED_KEY, EC_3_2, data);
    createOpenContainerKey(RATIS_OPEN_KEY, RATIS_THREE, data);
    createOpenContainerKey(EC_OPEN_KEY, EC_3_2, data);
  }

  /** Create a key and wait for all of its containers to close. */
  private void createClosedContainerKey(String keyName, ReplicationConfig repl, byte[] data) throws Exception {
    List<Long> containerIds;
    try (OzoneOutputStream out = bucket.createKey(keyName, data.length, repl, new HashMap<>())) {
      out.write(data);
      containerIds = ((KeyOutputStream) out.getOutputStream()).getStreamEntries().stream()
          .map(entry -> entry.getBlockID().getContainerID())
          .distinct()
          .collect(Collectors.toList());
    }
    OzoneTestHelper.waitForContainerClose(cluster, containerIds.toArray(new Long[0]));
  }

  /** Create a key whose container stays open. */
  private void createOpenContainerKey(String keyName, ReplicationConfig repl, byte[] data) throws IOException {
    try (OzoneOutputStream out = bucket.createKey(keyName, data.length, repl, new HashMap<>())) {
      out.write(data);
    }
  }

  @AfterAll
  void shutdown() throws IOException {
    if (client != null) {
      client.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  static List<HDDSVersion> currentVersions() {
    // UNKNOWN_VERSION is a placeholder, it is not meant to be returned from Datanodes to clients.
    return Arrays.stream(HDDSVersion.values())
        .filter(v -> !v.equals(HDDSVersion.UNKNOWN_VERSION))
        .collect(Collectors.toList());
  }

  @ParameterizedTest
  @MethodSource("currentVersions")
  public void testCurrentVersionReachesClient(HDDSVersion version) throws Exception {
    reloadDatanodeCurrentVersion(version);

    // Wait for the reloaded version to propagate to SCM via a heartbeat. Once it
    // has, it is stable: every datanode is now advertising this version.
    GenericTestUtils.waitFor(() -> scmReportsForAllDatanodes(version),
        100, PROPAGATION_TIMEOUT_MS);

    assertNodesAt(version, writePathPipeline("write-probe", RATIS_THREE), "RATIS write");
    assertNodesAt(version, writePathPipeline("ec-write-probe", EC_3_2), "EC write");
    assertNodesAt(version, readPathPipeline(RATIS_CLOSED_KEY), "RATIS closed-container read");
    assertNodesAt(version, readPathPipeline(EC_CLOSED_KEY), "EC closed-container read");
    assertNodesAt(version, readPathPipeline(RATIS_OPEN_KEY), "RATIS open-container read");
    assertNodesAt(version, readPathPipeline(EC_OPEN_KEY), "EC open-container read");
  }

  /**
   * Reload the "current version" test config on every live datanode and nudge a
   * heartbeat. The heartbeat task re-reads this config each heartbeat, so the
   * datanodes begin advertising {@code version} without being restarted.
   */
  private void reloadDatanodeCurrentVersion(HDDSVersion version) {
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      dn.getConf().setInt(TESTING_DATANODE_VERSION_CURRENT, version.serialize());
      // triggerHeartbeat re-reads the config at each invocation.
      dn.getDatanodeStateMachine().triggerHeartbeat();
    }
  }

  /** True once SCM's stored version for every datanode matches {@code version}. */
  private boolean scmReportsForAllDatanodes(HDDSVersion version) {
    NodeManager nodeManager = scm.getScmNodeManager();
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      DatanodeInfo info = nodeManager.getNode(dn.getDatanodeDetails().getID());
      if (info == null || info.getCurrentVersion() != version) {
        return false;
      }
    }
    return true;
  }

  /** Client write: the pipeline SCM hands back for a freshly allocated block. */
  private Pipeline writePathPipeline(String probeKey, ReplicationConfig repl) throws IOException {
    try (OzoneOutputStream out = bucket.createKey(probeKey, 1, repl, new HashMap<>())) {
      out.write(new byte[] {1});
      return ((KeyOutputStream) out.getOutputStream())
          .getStreamEntries().get(0).getPipeline();
    }
  }

  /** Client read: the pipeline OM resolves from SCM for the given key. */
  private Pipeline readPathPipeline(String keyName) throws IOException {
    OmKeyArgs args = new OmKeyArgs.Builder()
        .setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME)
        .setKeyName(keyName)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(args);
    return keyInfo.getLatestVersionLocations().getLocationList().get(0).getPipeline();
  }

  private static void assertNodesAt(HDDSVersion version, Pipeline pipeline, String path) {
    for (DatanodeDetails node : pipeline.getNodes()) {
      assertEquals(version, node.getCurrentVersion(),
          path + " path: datanode " + node.getID() + " reported the wrong currentVersion");
    }
  }
}
