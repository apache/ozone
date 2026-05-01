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

package org.apache.hadoop.ozone.debug;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SNAPSHOT_INFO_TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.debug.ldb.RDBParser;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Integration tests for dropping Ozone column families with ozone debug ldb.
 */
public class TestLDBDropColumnFamilyWithSnapshots {

  private MiniOzoneCluster cluster;
  private OzoneClient client;

  @AfterEach
  public void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void dropsOrganicallyPopulatedSnapshotInfoTable() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setEnum(HDDS_DB_PROFILE, DBProfile.TEST);
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String snapshotName1 = "snap1";
    String snapshotName2 = "snap2";
    ObjectStore objectStore = client.getObjectStore();
    TestDataUtil.createVolumeAndBucket(client, volumeName, bucketName,
        BucketLayout.DEFAULT);

    objectStore.createSnapshot(volumeName, bucketName, snapshotName1);
    objectStore.createSnapshot(volumeName, bucketName, snapshotName2);

    assertSnapshotInfoExists(volumeName, bucketName, snapshotName1);
    assertSnapshotInfoExists(volumeName, bucketName, snapshotName2);
    GenericTestUtils.waitFor(() -> hasSnapshotInfoRowCount(2),
        100, 10_000);
    assertEquals(2, countSnapshotInfoRows());

    File omDbPath = cluster.getOzoneManager().getMetadataManager()
        .getStore().getDbLocation();
    assertThat(listColumnFamilies(omDbPath)).contains(SNAPSHOT_INFO_TABLE);

    IOUtils.closeQuietly(client);
    client = null;
    cluster.stop();

    StringWriter stdout = new StringWriter();
    StringWriter stderr = new StringWriter();
    CommandLine cmd = new CommandLine(new RDBParser())
        .setOut(new PrintWriter(stdout))
        .setErr(new PrintWriter(stderr));

    int exitCode = cmd.execute(
        "--db", omDbPath.getAbsolutePath(),
        "drop_column_family",
        "--cf", SNAPSHOT_INFO_TABLE,
        "-y");

    assertEquals(0, exitCode, stderr.toString());
    assertThat(stdout.toString())
        .contains("Dropped column family " + SNAPSHOT_INFO_TABLE);
    assertThat(listColumnFamilies(omDbPath))
        .contains(KEY_TABLE)
        .doesNotContain(SNAPSHOT_INFO_TABLE);
  }

  private void assertSnapshotInfoExists(String volumeName, String bucketName,
      String snapshotName) throws Exception {
    SnapshotInfo snapshotInfo = cluster.getOzoneManager()
        .getMetadataManager()
        .getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName));
    assertNotNull(snapshotInfo);
  }

  private boolean hasSnapshotInfoRowCount(long expected) {
    try {
      return countSnapshotInfoRows() == expected;
    } catch (Exception ex) {
      throw new AssertionError(ex);
    }
  }

  private long countSnapshotInfoRows() throws Exception {
    return cluster.getOzoneManager().getMetadataManager()
        .countRowsInTable(cluster.getOzoneManager().getMetadataManager()
            .getSnapshotInfoTable());
  }

  private static List<String> listColumnFamilies(File dbLocation)
      throws Exception {
    List<String> columnFamilies = new ArrayList<>();
    for (byte[] columnFamily
        : RocksDatabase.listColumnFamiliesEmptyOptions(
            dbLocation.getAbsolutePath())) {
      columnFamilies.add(new String(columnFamily, UTF_8));
    }
    return columnFamilies;
  }
}
