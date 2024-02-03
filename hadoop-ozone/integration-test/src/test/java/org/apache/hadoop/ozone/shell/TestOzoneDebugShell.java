/*
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

package org.apache.hadoop.ozone.shell;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneSnapshot;
import org.apache.hadoop.ozone.debug.DBScanner;
import org.apache.hadoop.ozone.debug.OzoneDebug;
import org.apache.hadoop.ozone.debug.RDBParser;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.UUID;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test Ozone Debug shell.
 */
public class TestOzoneDebugShell {

  private static String omServiceId;
  private static String clusterId;
  private static String scmId;

  private static MiniOzoneCluster cluster = null;
  private static OzoneClient client;

  private static OzoneConfiguration conf = null;

  protected static void startCluster() throws Exception {
    // Init HA cluster
    omServiceId = "om-service-test1";
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    final int numDNs = 3;
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setNumDatanodes(numDNs)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
  }


  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, SECONDS);
    ReplicationManager.ReplicationManagerConfiguration replicationConf =
        conf.getObject(
            ReplicationManager.ReplicationManagerConfiguration.class);
    replicationConf.setInterval(Duration.ofSeconds(1));
    conf.setFromObject(replicationConf);
    startCluster();
  }

  @Test
  public void testChunkInfoCmdBeforeAfterCloseContainer() throws Exception {
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();
    final String keyName = UUID.randomUUID().toString();

    writeKey(volumeName, bucketName, keyName);

    int exitCode = runChunkInfoCommand(volumeName, bucketName, keyName);
    assertEquals(0, exitCode);

    closeContainerForKey(volumeName, bucketName, keyName);

    exitCode = runChunkInfoCommand(volumeName, bucketName, keyName);
    assertEquals(0, exitCode);
  }

  @Test
  public void testChunkInfoVerifyPathsAreDifferent() throws Exception {
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();
    final String keyName = UUID.randomUUID().toString();
    writeKey(volumeName, bucketName, keyName);
    int exitCode = runChunkInfoAndVerifyPaths(volumeName, bucketName, keyName);
    assertEquals(0, exitCode);
  }

  @Test
  public void testLdbCliForOzoneSnapshot() throws Exception {
    StringWriter stdout = new StringWriter();
    PrintWriter pstdout = new PrintWriter(stdout);
    CommandLine cmd = new CommandLine(new RDBParser())
        .addSubcommand(new DBScanner())
        .setOut(pstdout);
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();
    final String keyName = UUID.randomUUID().toString();

    writeKey(volumeName, bucketName, keyName);

    String snapshotName =
        client.getObjectStore().createSnapshot(volumeName, bucketName, "snap1");
    OzoneSnapshot snapshot =
        client.getObjectStore().listSnapshot(volumeName, bucketName, null, null)
            .next();
    assertEquals(snapshotName, snapshot.getName());
    String dbPath = getSnapshotDBPath(snapshot.getCheckpointDir());
    String snapshotCurrent = dbPath + OM_KEY_PREFIX + "CURRENT";
    GenericTestUtils
        .waitFor(() -> new File(snapshotCurrent).exists(), 1000, 120000);
    String[] args =
        new String[] {"--db=" + dbPath, "scan", "--cf", "keyTable"};
    int exitCode = cmd.execute(args);
    assertEquals(0, exitCode);
    String cmdOut = stdout.toString();
    assertThat(cmdOut).contains(keyName);
  }

  private static String getSnapshotDBPath(String checkPointDir) {
    return OMStorage.getOmDbDir(conf) +
        OM_KEY_PREFIX + OM_SNAPSHOT_CHECKPOINT_DIR + OM_KEY_PREFIX +
        OM_DB_NAME + checkPointDir;
  }


  private static void writeKey(String volumeName, String bucketName,
      String keyName) throws IOException {
    try (OzoneClient client = OzoneClientFactory.getRpcClient(conf)) {
      // see HDDS-10091 for making this work with FILE_SYSTEM_OPTIMIZED layout
      TestDataUtil.createVolumeAndBucket(client, volumeName, bucketName, BucketLayout.LEGACY);
      TestDataUtil.createKey(
          client.getObjectStore().getVolume(volumeName).getBucket(bucketName),
          keyName, ReplicationFactor.THREE, ReplicationType.RATIS, "test");
    }
  }

  private int runChunkInfoCommand(String volumeName, String bucketName,
      String keyName) {
    String bucketPath =
        Path.SEPARATOR + volumeName + Path.SEPARATOR + bucketName;
    String[] args = new String[] {
        getSetConfStringFromConf(OMConfigKeys.OZONE_OM_ADDRESS_KEY),
        "chunkinfo", bucketPath + Path.SEPARATOR + keyName };

    OzoneDebug ozoneDebugShell = new OzoneDebug(conf);
    int exitCode = ozoneDebugShell.execute(args);
    return exitCode;
  }

  private int runChunkInfoAndVerifyPaths(String volumeName, String bucketName,
      String keyName) throws Exception {
    String bucketPath =
        Path.SEPARATOR + volumeName + Path.SEPARATOR + bucketName;
    String[] args = new String[] {
        getSetConfStringFromConf(OMConfigKeys.OZONE_OM_ADDRESS_KEY),
        "chunkinfo", bucketPath + Path.SEPARATOR + keyName };
    OzoneDebug ozoneDebugShell = new OzoneDebug(conf);
    int exitCode = 1;
    try (GenericTestUtils.SystemOutCapturer capture = new GenericTestUtils
        .SystemOutCapturer()) {
      exitCode = ozoneDebugShell.execute(args);
      Set<String> blockFilePaths = new HashSet<>();
      String output = capture.getOutput();
      ObjectMapper objectMapper = new ObjectMapper();
      // Parse the JSON array string into a JsonNode
      JsonNode jsonNode = objectMapper.readTree(output);
      JsonNode keyLocations = jsonNode.get("KeyLocations").get(0);
      for (JsonNode element : keyLocations) {
        String fileName =
            element.get("Locations").get("files").get(0).toString();
        blockFilePaths.add(fileName);
      }
      // DN storage directories are set differently for each DN
      // in MiniOzoneCluster as datanode-0,datanode-1,datanode-2 which is why
      // we expect 3 paths here in the set.
      assertEquals(3, blockFilePaths.size());
    }
    return exitCode;
  }

  /**
   * Generate string to pass as extra arguments to the
   * ozone debug command line, This is necessary for client to
   * connect to OM by setting the right om address.
   */
  private String getSetConfStringFromConf(String key) {
    return String.format("--set=%s=%s", key, conf.get(key));
  }

  private static void closeContainerForKey(String volumeName, String bucketName,
      String keyName)
      throws IOException, TimeoutException, InterruptedException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName).setKeyName(keyName).build();

    OmKeyLocationInfo omKeyLocationInfo =
        cluster.getOzoneManager().lookupKey(keyArgs).getKeyLocationVersions()
            .get(0).getBlocksLatestVersionOnly().get(0);

    ContainerInfo container =
        cluster.getStorageContainerManager().getContainerManager().getContainer(
            ContainerID.valueOf(omKeyLocationInfo.getContainerID()));
    OzoneTestUtils.closeContainer(cluster.getStorageContainerManager(),
        container);
  }

  /**
   * shutdown MiniOzoneCluster.
   */
  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
