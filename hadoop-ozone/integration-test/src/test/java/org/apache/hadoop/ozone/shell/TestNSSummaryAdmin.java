/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.shell;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.StandardOutputTestBase;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.ozone.test.JUnit5AwareTimeout;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.io.UnsupportedEncodingException;
import java.util.UUID;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_KEY;

/**
 * Test for Namespace CLI.
 */
public class TestNSSummaryAdmin extends StandardOutputTestBase {
  private static ObjectStore store;

  private static OzoneAdmin ozoneAdmin;
  private static OzoneConfiguration conf;
  private static MiniOzoneCluster cluster;

  private static String volumeName;
  private static String bucketOBS;
  private static String bucketFSO;
  private static OzoneClient client;

  @Rule
  public TestRule timeout = new JUnit5AwareTimeout(Timeout.seconds(60));

  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    OMRequestTestUtils.configureFSOptimizedPaths(conf, true);
    conf.set(OZONE_RECON_ADDRESS_KEY, "localhost:9888");
    cluster = MiniOzoneCluster.newBuilder(conf)
        .withoutDatanodes().includeRecon(true).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    store = client.getObjectStore();

    // Client uses server conf for this test
    ozoneAdmin = new OzoneAdmin(conf);

    volumeName = UUID.randomUUID().toString();
    bucketOBS = UUID.randomUUID().toString();
    bucketFSO = UUID.randomUUID().toString();
    createVolumeAndBuckets();
  }

  @AfterClass
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Create OBS and FSO buckets for the tests.
   * @throws Exception
   */
  private static void createVolumeAndBuckets()
      throws Exception {
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);

    // Create OBS bucket.
    BucketArgs bucketArgsOBS = BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.OBJECT_STORE)
        .build();
    volume.createBucket(bucketOBS, bucketArgsOBS);

    // Create FSO bucket.
    BucketArgs bucketArgsFSO = BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .build();
    volume.createBucket(bucketFSO, bucketArgsFSO);
  }

  /**
   * Test NSSummaryCLI on root path.
   */
  @Test
  public void testNSSummaryCLIRoot() throws UnsupportedEncodingException {
    // Running on root path.
    String path = "/";
    executeAdminCommands(path);
    // Should throw warning - only buckets can have bucket layout.
    Assert.assertTrue(
        getOutContentString().contains(
            "[Warning] Namespace CLI is not designed for OBS bucket layout."));
    Assert.assertTrue(getOutContentString()
        .contains("Put more files into it to visualize DU"));
    Assert.assertTrue(getOutContentString().contains(
        "Put more files into it to visualize file size distribution"));
  }

  /**
   * Test NSSummaryCLI on FILE_SYSTEM_OPTIMIZED bucket.
   */
  @Test
  public void testNSSummaryCLIFSO() throws UnsupportedEncodingException {
    // Running on FSO Bucket.
    String path = "/" + volumeName + "/" + bucketFSO;
    executeAdminCommands(path);
    // Should not throw warning, since bucket is in FSO bucket layout.
    Assert.assertFalse(
        getOutContentString().contains(
            "[Warning] Namespace CLI is not designed for OBS bucket layout."));
    Assert.assertTrue(getOutContentString()
        .contains("Put more files into it to visualize DU"));
    Assert.assertTrue(getOutContentString().contains(
        "Put more files into it to visualize file size distribution"));
  }

  /**
   * Test NSSummaryCLI on OBJECT_STORE bucket.
   */
  @Test
  public void testNSSummaryCLIOBS() throws UnsupportedEncodingException {
    // Running on OBS Bucket.
    String path = "/" + volumeName + "/" + bucketOBS;
    executeAdminCommands(path);
    // Should throw warning, since bucket is in OBS bucket layout.
    Assert.assertTrue(
        getOutContentString().contains(
            "[Warning] Namespace CLI is not designed for OBS bucket layout."));
    Assert.assertTrue(getOutContentString()
        .contains("Put more files into it to visualize DU"));
    Assert.assertTrue(getOutContentString().contains(
        "Put more files into it to visualize file size distribution"));
  }

  /**
   * Execute ozoneAdmin commands on given path.
   *
   * @param path
   */
  private void executeAdminCommands(String path) {
    String[] summaryArgs = {"namespace", "summary", path};
    String[] duArgs = {"namespace", "du", path};
    String[] duArgsWithOps =
        {"namespace", "du", "-rfn", "--length=100", path};
    String[] quotaArgs = {"namespace", "quota", path};
    String[] distArgs = {"namespace", "dist", path};

    ozoneAdmin.execute(summaryArgs);
    ozoneAdmin.execute(duArgs);
    ozoneAdmin.execute(duArgsWithOps);
    ozoneAdmin.execute(quotaArgs);
    ozoneAdmin.execute(distArgs);
  }
}
