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

package org.apache.hadoop.ozone.repair.om;

import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_HTTP_ENDPOINT_V2;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.apache.ozone.test.OzoneTestBase.uniqueObjectName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneSnapshot;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis_snapshot.OmRatisSnapshotProvider;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for `ozone repair om download`.
 */
public class TestDownloadOMDBTool {

  private static final String OM_SERVICE_ID = "om-service-download-test";
  private static MiniOzoneHAClusterImpl cluster;
  private static OzoneConfiguration conf;

  private GenericTestUtils.PrintStreamCapturer out;
  private GenericTestUtils.PrintStreamCapturer err;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(OM_SERVICE_ID)
        .setNumOfOzoneManagers(3)
        .setNumOfActiveOMs(3)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterAll
  public static void cleanup() {
    IOUtils.closeQuietly(cluster);
  }

  @BeforeEach
  public void setup() {
    out = GenericTestUtils.captureOut();
    err = GenericTestUtils.captureErr();
  }

  @AfterEach
  public void reset() {
    IOUtils.closeQuietly(out, err);
  }

  @Test
  public void testDownloadConstructsOmDbUsingV2Transfer(@TempDir Path tempDir) throws Exception {
    Path outputDir = tempDir.resolve("downloaded-metadata");

    LogCapturer providerLog = LogCapturer.captureLogs(OmRatisSnapshotProvider.class);
    try {
      int exitCode = new OzoneRepair().getCmd().execute(withHAConf(new String[] {
          "om", "download",
          "--service-id", OM_SERVICE_ID,
          "--output-dir", outputDir.toString()
      }));

      assertEquals(0, exitCode, err.getOutput());
      Path omDbDir = outputDir.resolve(OM_DB_NAME);
      assertTrue(Files.isDirectory(omDbDir), "Expected downloaded om.db directory to exist.");
      assertTrue(Files.exists(omDbDir.resolve("CURRENT")), "Expected RocksDB CURRENT file in downloaded om.db.");
      assertThat(providerLog.getOutput()).contains(OZONE_DB_CHECKPOINT_HTTP_ENDPOINT_V2);
    } finally {
      providerLog.stopCapturing();
    }
  }

  @Test
  public void testDownloadPreservesDbSnapshots(@TempDir Path tempDir) throws Exception {
    String volumeName = uniqueObjectName("vol");
    String bucketName = uniqueObjectName("buck");
    String snapshotName = uniqueObjectName("snap");
    String keyName = uniqueObjectName("key");

    OzoneClient client = cluster.newClient();
    try {
      ObjectStore store = client.getObjectStore();
      OzoneBucket bucket = DataTestUtil.createVolumeAndBucket(client, volumeName, bucketName);
      DataTestUtil.createKey(bucket, keyName, "snapshot-payload".getBytes(StandardCharsets.UTF_8));

      store.createSnapshot(volumeName, bucketName, snapshotName);

      OzoneSnapshot createdSnapshot = null;
      Iterator<OzoneSnapshot> snapshots =
          store.listSnapshot(volumeName, bucketName, "", null);
      while (snapshots.hasNext()) {
        OzoneSnapshot snapshot = snapshots.next();
        if (snapshotName.equals(snapshot.getName())) {
          createdSnapshot = snapshot;
          break;
        }
      }
      assertNotNull(createdSnapshot, "Expected Ozone snapshot to exist before download.");
      UUID snapshotId = createdSnapshot.getSnapshotId();

      OzoneManager leader = cluster.getOMLeader();
      String leaderNodeId = leader.getOMNodeId();
      Path outputDir = tempDir.resolve("downloaded-metadata-with-snapshots");

      int exitCode = new OzoneRepair().getCmd().execute(withHAConf(new String[] {
          "om", "download",
          "--service-id", OM_SERVICE_ID,
          "--node-id", leaderNodeId,
          "--output-dir", outputDir.toString()
      }));

      assertEquals(0, exitCode, err.getOutput());
      Path omDbDir = outputDir.resolve(OM_DB_NAME);
      Path snapshotsDir = outputDir.resolve(OM_SNAPSHOT_DIR);
      assertTrue(Files.isDirectory(omDbDir));
      assertTrue(Files.exists(omDbDir.resolve("CURRENT")));
      assertTrue(Files.isDirectory(snapshotsDir),
          "Expected db.snapshots in downloaded metadata layout.");
      Path checkpointState = snapshotsDir.resolve("checkpointState");
      assertTrue(Files.isDirectory(checkpointState),
          "Expected db.snapshots/checkpointState in downloaded metadata.");

      String snapshotCheckpointPrefix = OM_DB_NAME + "-" + snapshotId;
      Path snapshotYaml = checkpointState.resolve(snapshotCheckpointPrefix + ".yaml");
      Path snapshotCheckpointDir = checkpointState.resolve(snapshotCheckpointPrefix);
      assertTrue(Files.exists(snapshotYaml),
          "Expected snapshot checkpoint YAML for created Ozone snapshot.");
      assertTrue(Files.isDirectory(snapshotCheckpointDir),
          "Expected snapshot RocksDB checkpoint dir for created Ozone snapshot.");
    } finally {
      IOUtils.closeQuietly(client);
    }
  }

  @Test
  public void testOverwriteReplacesExistingOutput(@TempDir Path tempDir) throws Exception {
    OzoneManager leader = cluster.getOMLeader();
    String leaderNodeId = leader.getOMNodeId();
    Path outputDir = tempDir.resolve("downloaded-metadata");
    Files.createDirectories(outputDir);
    Files.write(outputDir.resolve("OLD-MARKER"), "stale".getBytes(StandardCharsets.UTF_8));

    int firstRunExitCode = new OzoneRepair().getCmd().execute(withHAConf(new String[] {
        "om", "download",
        "--service-id", OM_SERVICE_ID,
        "--node-id", leaderNodeId,
        "--output-dir", outputDir.toString()
    }));
    assertNotEquals(0, firstRunExitCode);
    assertTrue(Files.exists(outputDir.resolve("OLD-MARKER")), "Marker should remain without overwrite.");
    assertThat(err.getOutput()).contains("Output directory already exists");

    out.reset();
    err.reset();
    int secondRunExitCode = new OzoneRepair().getCmd().execute(withHAConf(new String[] {
        "om", "download",
        "--service-id", OM_SERVICE_ID,
        "--node-id", leaderNodeId,
        "--output-dir", outputDir.toString(),
        "--overwrite"
    }));
    assertEquals(0, secondRunExitCode, err.getOutput());
    assertFalse(Files.exists(outputDir.resolve("OLD-MARKER")), "Overwrite should remove stale output.");
    assertTrue(Files.exists(outputDir.resolve(OM_DB_NAME).resolve("CURRENT")),
        "Downloaded om.db should exist after overwrite.");
  }

  private String[] withHAConf(String[] existingArgs) throws IOException {
    List<String> args = new ArrayList<>();
    addConf(args, OZONE_OM_SERVICE_IDS_KEY);

    String omNodesKey = ConfUtils.addKeySuffixes(OZONE_OM_NODES_KEY, OM_SERVICE_ID);
    addConf(args, omNodesKey);

    Collection<String> omNodes = conf.getTrimmedStringCollection(omNodesKey);
    for (String omNodeId : omNodes) {
      addConf(args, ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, OM_SERVICE_ID, omNodeId));
      addOptionalConf(args, ConfUtils.addKeySuffixes(OZONE_OM_HTTP_ADDRESS_KEY, OM_SERVICE_ID, omNodeId));
    }

    addOptionalConf(args, OzoneConfigKeys.OZONE_HTTP_POLICY_KEY);

    args.addAll(Arrays.asList(existingArgs));
    return args.toArray(new String[0]);
  }

  private void addConf(List<String> args, String key) throws IOException {
    String value = conf.get(key);
    if (value == null || value.isEmpty()) {
      throw new IOException("Missing required config key for CLI test: " + key);
    }
    args.add("-D");
    args.add(key + "=" + value);
  }

  private void addOptionalConf(List<String> args, String key) {
    String value = conf.get(key);
    if (value == null || value.isEmpty()) {
      return;
    }
    args.add("-D");
    args.add(key + "=" + value);
  }
}
