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

package org.apache.hadoop.ozone.shell;

import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneSnapshot;
import org.apache.hadoop.ozone.debug.OzoneDebug;
import org.apache.hadoop.ozone.debug.ldb.RDBParser;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

/**
 * Test Ozone Debug shell.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestOzoneDebugShell implements NonHATests.TestCase {

  private OzoneClient client;
  private OzoneDebug ozoneDebugShell;
  private OMMetadataManager omMetadataManager;

  @BeforeEach
  void init() throws Exception {
    ozoneDebugShell = new OzoneDebug();
    client = cluster().newClient();
    omMetadataManager = cluster().getOzoneManager().getMetadataManager();
  }

  @AfterEach
  void shutdown() {
    IOUtils.closeQuietly(client);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testReplicasVerifyCmd(boolean isEcKey) throws Exception {
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();
    final String keyName = UUID.randomUUID().toString();

    writeKey(volumeName, bucketName, keyName, isEcKey, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    String bucketPath = Path.SEPARATOR + volumeName + Path.SEPARATOR + bucketName;
    String fullKeyPath = bucketPath + Path.SEPARATOR + keyName;

    //TODO HDDS-12715: Create common integration test cluster for debug and repair tools
    String[] args = new String[] {
        getSetConfStringFromConf(OMConfigKeys.OZONE_OM_ADDRESS_KEY),
        getSetConfStringFromConf(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY),
        "replicas", "verify", "--checksums", "--block-existence", "--container-state", fullKeyPath,
        //, "--all-results"
    };

    int exitCode = ozoneDebugShell.execute(args);
    assertEquals(0, exitCode);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testChunkInfoCmdBeforeAfterCloseContainer(boolean isEcKey) throws Exception {
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();
    final String keyName = UUID.randomUUID().toString();

    writeKey(volumeName, bucketName, keyName, isEcKey, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    int exitCode = runChunkInfoCommand(volumeName, bucketName, keyName);
    assertEquals(0, exitCode);

    closeContainerForKey(volumeName, bucketName, keyName);

    exitCode = runChunkInfoCommand(volumeName, bucketName, keyName);
    assertEquals(0, exitCode);
  }

  @ParameterizedTest
  @EnumSource
  public void testChunkInfoVerifyPathsAreDifferent(BucketLayout layout) throws Exception {
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();
    final String keyName = UUID.randomUUID().toString();
    writeKey(volumeName, bucketName, keyName, false, layout);
    int exitCode = runChunkInfoAndVerifyPaths(volumeName, bucketName, keyName);
    assertEquals(0, exitCode);
  }

  @ParameterizedTest
  @EnumSource
  public void testLdbCliForOzoneSnapshot(BucketLayout layout) throws Exception {
    String columnFamily = omMetadataManager.getKeyTable(layout).getName();
    StringWriter stdout = new StringWriter();
    PrintWriter pstdout = new PrintWriter(stdout);
    CommandLine cmd = new CommandLine(new RDBParser())
        .setOut(pstdout);
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();
    final String keyName = UUID.randomUUID().toString();

    writeKey(volumeName, bucketName, keyName, false, layout);

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
        new String[] {"--db=" + dbPath, "scan", "--cf", columnFamily};
    int exitCode = cmd.execute(args);
    assertEquals(0, exitCode);
    String cmdOut = stdout.toString();
    assertThat(cmdOut).contains(keyName);
  }

  private String getSnapshotDBPath(String checkPointDir) {
    return OMStorage.getOmDbDir(cluster().getConf()) +
        OM_KEY_PREFIX + OM_SNAPSHOT_CHECKPOINT_DIR + OM_KEY_PREFIX +
        OM_DB_NAME + checkPointDir;
  }

  private void writeKey(String volumeName, String bucketName,
      String keyName, boolean isEcKey, BucketLayout layout) throws IOException {
    ReplicationConfig repConfig;
    if (isEcKey) {
      repConfig = new ECReplicationConfig(3, 2);
    } else {
      repConfig = ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
          ReplicationFactor.THREE);
    }
    TestDataUtil.createVolumeAndBucket(client, volumeName, bucketName,
        layout);
    TestDataUtil.createKey(
        client.getObjectStore().getVolume(volumeName).getBucket(bucketName),
        keyName, repConfig, "test".getBytes(StandardCharsets.UTF_8));
  }

  private int runChunkInfoCommand(String volumeName, String bucketName,
      String keyName) {
    String bucketPath =
        Path.SEPARATOR + volumeName + Path.SEPARATOR + bucketName;
    String[] args = new String[] {
        getSetConfStringFromConf(OMConfigKeys.OZONE_OM_ADDRESS_KEY),
        getSetConfStringFromConf(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY),
        "replicas", "chunk-info", bucketPath + Path.SEPARATOR + keyName };

    return ozoneDebugShell.execute(args);
  }

  private int runChunkInfoAndVerifyPaths(String volumeName, String bucketName,
      String keyName) throws Exception {
    int exitCode = 1;
    try (GenericTestUtils.SystemOutCapturer capture = new GenericTestUtils
        .SystemOutCapturer()) {
      exitCode = runChunkInfoCommand(volumeName, bucketName, keyName);
      Set<String> blockFilePaths = new HashSet<>();
      String output = capture.getOutput();
      ObjectMapper objectMapper = new ObjectMapper();
      // Parse the JSON array string into a JsonNode
      JsonNode jsonNode = objectMapper.readTree(output);
      JsonNode keyLocations = jsonNode.get("keyLocations").get(0);
      for (JsonNode element : keyLocations) {
        String fileName =
            element.get("file").toString();
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
    return String.format("--set=%s=%s", key, cluster().getConf().get(key));
  }

  private void closeContainerForKey(String volumeName, String bucketName,
      String keyName)
      throws IOException, TimeoutException, InterruptedException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName).setKeyName(keyName).build();

    OmKeyLocationInfo omKeyLocationInfo =
        cluster().getOzoneManager().lookupKey(keyArgs).getKeyLocationVersions()
            .get(0).getBlocksLatestVersionOnly().get(0);

    ContainerInfo container =
        cluster().getStorageContainerManager().getContainerManager().getContainer(
            ContainerID.valueOf(omKeyLocationInfo.getContainerID()));
    OzoneTestUtils.closeContainer(cluster().getStorageContainerManager(),
        container);
  }
}
