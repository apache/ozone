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
package org.apache.hadoop.fs.ozone;

import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.junit.Assert.assertEquals;

/**
 * Test client-side CRUD snapshot operations with Ozone Manager.
 */
public class TestOzoneFsSnapshot {
  // Set the timeout for every test.
  @Rule
  public Timeout testTimeout = Timeout.seconds(300);

  private static MiniOzoneCluster cluster;
  private static final String OM_SERVICE_ID = "om-service-test1";
  private OzoneConfiguration clientConf;
  private static OzoneManager ozoneManager;

  @BeforeClass
  public static void initClass() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    // Start the cluster
    cluster = MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(UUID.randomUUID().toString())
        .setScmId(UUID.randomUUID().toString())
        .setOMServiceId(OM_SERVICE_ID)
        .setNumOfOzoneManagers(1)
        .build();
    cluster.waitForClusterToBeReady();
    ozoneManager = cluster.getOzoneManager();
  }

  @Before
  public void init() {
    String hostPrefix = OZONE_OFS_URI_SCHEME + "://" + OM_SERVICE_ID;
    clientConf = new OzoneConfiguration(cluster.getConf());
    clientConf.set(FS_DEFAULT_NAME_KEY, hostPrefix);
  }

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testCreateSnapshot() throws Exception {
    String volume = "vol1";
    String bucket = "bucket1";
    String testVolBucket = OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket;
    String snapshotName = "snap1";
    String testKey = testVolBucket + "/key1";

    OzoneFsShell shell = new OzoneFsShell(clientConf);
    try {
      // Create volume and bucket
      int res = ToolRunner.run(shell,
          new String[]{"-mkdir", "-p", testVolBucket});
      assertEquals(0, res);
      // Create key
      ToolRunner.run(shell, new String[]{"-touch", testKey});
      assertEquals(0, res);
      // List the bucket to make sure that bucket exists.
      ToolRunner.run(shell, new String[]{"-ls", testVolBucket});
      assertEquals(0, res);

      res = ToolRunner.run(shell,
          new String[]{"-createSnapshot", testVolBucket, snapshotName});
      // Asserts that create request succeeded
      assertEquals(0, res);

      SnapshotInfo snapshotInfo = ozoneManager
          .getMetadataManager()
          .getSnapshotInfoTable()
          .get(SnapshotInfo.getTableKey(volume, bucket, snapshotName));

      // Assert that snapshot exists in RocksDB.
      // We can't use list or valid if snapshot directory exists because DB
      // transaction might not be flushed by the time.
      Assert.assertNotNull(snapshotInfo);
    } finally {
      shell.close();
    }
  }
}
