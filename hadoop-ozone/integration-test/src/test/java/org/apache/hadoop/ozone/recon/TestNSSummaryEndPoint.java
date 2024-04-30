/**
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
package org.apache.hadoop.ozone.recon;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.event.Level;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_RECON_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DELAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for NSSummaryEndPoint APIs.
 */
@Timeout(300)
class TestNSSummaryEndPoint {

  private static OzoneBucket legacyOzoneBucket;
  private static OzoneBucket fsoOzoneBucket;
  private static OzoneBucket obsOzoneBucket;
  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static MiniOzoneCluster cluster;
  private static NodeManager scmNodeManager;
  private static ContainerManager scmContainerManager;

  @BeforeAll
  static void init() throws Exception {
    setupConfigKeys();
    cluster = MiniOzoneCluster.newBuilder(CONF)
                  .setNumDatanodes(5)
                  .includeRecon(true)
                  .build();
    cluster.waitForClusterToBeReady();
    GenericTestUtils.setLogLevel(ReconNodeManager.LOG, Level.DEBUG);

    StorageContainerManager scm = cluster.getStorageContainerManager();
    scmContainerManager = scm.getContainerManager();
    scmNodeManager = scm.getScmNodeManager();

    ReconStorageContainerManagerFacade reconScm =
        (ReconStorageContainerManagerFacade)
            cluster.getReconServer().getReconStorageContainerManager();
    PipelineManager reconPipelineManager = reconScm.getPipelineManager();

    LambdaTestUtils.await(60000, 5000,
        () -> (reconPipelineManager.getPipelines().size() >= 4));

    assertThat(scmContainerManager.getContainers()).isEmpty();

    // Verify that all nodes are registered with Recon.
    NodeManager reconNodeManager = reconScm.getScmNodeManager();
    assertEquals(scmNodeManager.getAllNodes().size(),
        reconNodeManager.getAllNodes().size());

    OzoneClient client = cluster.newClient();
    String volumeName = "vol1";
    String fsoBucketName = "fso-bucket";
    String legacyBucketName = "legacy-bucket";
    String obsBucketName = "obs-bucket";

    // create a volume and a FSO bucket
    fsoOzoneBucket = TestDataUtil.createVolumeAndBucket(
        client, volumeName, fsoBucketName, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    BucketArgs bucketArgs = new BucketArgs.Builder()
        .setBucketLayout(BucketLayout.LEGACY)
        .build();
    // create a LEGACY bucket
    legacyOzoneBucket = TestDataUtil
        .createBucket(client, volumeName, bucketArgs, legacyBucketName);

    bucketArgs = new BucketArgs.Builder()
        .setBucketLayout(BucketLayout.OBJECT_STORE)
        .build();
    // create a OBS bucket
    obsOzoneBucket = TestDataUtil
        .createBucket(client, volumeName, bucketArgs, obsBucketName);

    buildNameSpaceTree(obsOzoneBucket);
    buildNameSpaceTree(legacyOzoneBucket);
    buildNameSpaceTree(fsoOzoneBucket);
  }

  /**
   * Verify listKeys at different levels.
   */
  private static void buildNameSpaceTree(OzoneBucket ozoneBucket)
      throws Exception {
    LinkedList<String> keys = new LinkedList<>();
    keys.add("/a1/b1/c1111.tx");
    keys.add("/a1/b1/c1222.tx");
    keys.add("/a1/b1/c1333.tx");
    keys.add("/a1/b1/c1444.tx");
    keys.add("/a1/b1/c1555.tx");
    keys.add("/a1/b1/c1/c1.tx");
    keys.add("/a1/b1/c12/c2.tx");
    keys.add("/a1/b1/c12/c3.tx");

    keys.add("/a1/b2/d1/d11.tx");
    keys.add("/a1/b2/d2/d21.tx");
    keys.add("/a1/b2/d2/d22.tx");
    keys.add("/a1/b2/d3/d31.tx");

    keys.add("/a1/b3/e1/e11.tx");
    keys.add("/a1/b3/e2/e21.tx");
    keys.add("/a1/b3/e3/e31.tx");

    createKeys(ozoneBucket, keys);
  }

  private static void createKeys(OzoneBucket ozoneBucket, List<String> keys)
      throws Exception {
    int length = 10;
    byte[] input = new byte[length];
    Arrays.fill(input, (byte) 96);
    for (String key : keys) {
      createKey(ozoneBucket, key, 10, input);
    }
  }

  private static void createKey(OzoneBucket ozoneBucket, String key, int length,
                                byte[] input) throws Exception {

    OzoneOutputStream ozoneOutputStream =
        ozoneBucket.createKey(key, length);

    ozoneOutputStream.write(input);
    ozoneOutputStream.write(input, 0, 10);
    ozoneOutputStream.close();

    // Read the key with given key name.
    OzoneInputStream ozoneInputStream = ozoneBucket.readKey(key);
    byte[] read = new byte[length];
    ozoneInputStream.read(read, 0, length);
    ozoneInputStream.close();

    assertEquals(new String(input, StandardCharsets.UTF_8),
        new String(read, StandardCharsets.UTF_8));
  }

  @AfterAll
  static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static void setupConfigKeys() {
    CONF.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    CONF.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    CONF.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    CONF.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    CONF.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 1, SECONDS);
    CONF.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, SECONDS);
    CONF.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 1, SECONDS);
    CONF.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    CONF.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);
    CONF.setTimeDuration(OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL,
        1, SECONDS);
    CONF.setTimeDuration(
        ScmConfigKeys.OZONE_SCM_EXPIRED_CONTAINER_REPLICA_OP_SCRUB_INTERVAL,
        1, SECONDS);
    CONF.setTimeDuration(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        0, SECONDS);
    CONF.set(OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION, "2s");
    CONF.set(ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL, "2s");
    CONF.set(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, "5s");

    CONF.setTimeDuration(HDDS_RECON_HEARTBEAT_INTERVAL,
        1, TimeUnit.SECONDS);

    CONF.setTimeDuration(OZONE_RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY,
        1, TimeUnit.SECONDS);
    CONF.setTimeDuration(OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DELAY,
        2, TimeUnit.SECONDS);
  }

  @Test
  void testListKeysForFSOBucket() throws Exception {
    assertDirectKeysInFSOBucket();
    assertAllKeysRecursivelyInFSOBucket();
  }

  private static void assertDirectKeysInFSOBucket() throws JsonProcessingException, UnsupportedEncodingException {
    // assert direct keys inside fsoBucket
    DUResponse response = TestReconEndpointUtil.listKeysFromRecon(CONF, "/vol1/fso-bucket", false);
    // No direct keys,  total count provides all keys recursively in fso-bucket
    // but since we passed recursive as false, so no list of keys under duData subpaths.
    assertEquals(0, response.getCount());
    assertEquals(0, response.getDuData().size());
    assertEquals(15, response.getTotalCount());
  }

  private static void assertAllKeysRecursivelyInFSOBucket()
      throws JsonProcessingException, UnsupportedEncodingException {
    // assert direct keys inside fsoBucket
    DUResponse response = TestReconEndpointUtil.listKeysFromRecon(CONF, "/vol1/fso-bucket", true);
    // No direct keys,  total count provides all keys recursively in fso-bucket
    // but since we passed recursive as false, so no list of keys under duData subpaths.
    assertEquals(15, response.getCount());
    assertEquals(15, response.getDuData().size());
    assertEquals(15, response.getTotalCount());
    assertEquals("vol1/fso-bucket/a1/b1/c12/c3.tx", response.getDuData().get(14).getSubpath());
    assertEquals(300, response.getSize());
    assertEquals(900, response.getSizeWithReplica());
  }

  @Test
  void testListKeysForOBSBucket() throws Exception {
    // Both assertion should give same count of keys.
    assertDirectKeysInOBSBucket();
    assertAllKeysRecursivelyInOBSBucket();
  }

  private static void assertDirectKeysInOBSBucket() throws JsonProcessingException, UnsupportedEncodingException {
    // assert direct keys inside obs-bucket
    DUResponse response = TestReconEndpointUtil.listKeysFromRecon(CONF, "/vol1/obs-bucket", false);
    assertEquals(15, response.getCount());
    assertEquals(15, response.getDuData().size());
    assertEquals(15, response.getTotalCount());
  }

  private static void assertAllKeysRecursivelyInOBSBucket()
      throws JsonProcessingException, UnsupportedEncodingException {
    // assert all keys inside obs-bucket
    DUResponse response = TestReconEndpointUtil.listKeysFromRecon(CONF, "/vol1/obs-bucket", true);

    assertEquals(15, response.getCount());
    assertEquals(15, response.getDuData().size());
    assertEquals(15, response.getTotalCount());
    assertEquals("/a1/b3/e3/e31.tx", response.getDuData().get(14).getSubpath());
    assertEquals(300, response.getSize());
    assertEquals(900, response.getSizeWithReplica());
  }

  @Test
  void testListKeysForLegacyBucket() throws Exception {
    // Both assertion should give same count of keys.
    assertDirectKeysInLegacyBucket();
    assertAllKeysInLegacyBucket();
  }

  private static void assertDirectKeysInLegacyBucket() throws JsonProcessingException, UnsupportedEncodingException {
    // assert direct keys inside legacy-bucket
    DUResponse response = TestReconEndpointUtil.listKeysFromRecon(CONF, "/vol1/legacy-bucket", false);
    assertEquals(15, response.getCount());
    assertEquals(15, response.getDuData().size());
    assertEquals(15, response.getTotalCount());
  }

  private static void assertAllKeysInLegacyBucket()
      throws JsonProcessingException, UnsupportedEncodingException {
    // assert all keys inside legacy-bucket
    DUResponse response = TestReconEndpointUtil.listKeysFromRecon(CONF, "/vol1/legacy-bucket", true);

    assertEquals(15, response.getCount());
    assertEquals(15, response.getDuData().size());
    assertEquals(15, response.getTotalCount());
    assertEquals("/a1/b3/e3/e31.tx", response.getDuData().get(14).getSubpath());
    assertEquals(300, response.getSize());
    assertEquals(900, response.getSizeWithReplica());
  }
}
