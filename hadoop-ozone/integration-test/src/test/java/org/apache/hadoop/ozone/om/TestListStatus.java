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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.AfterClass;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.UUID;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConfigKeys.
    OZONE_FS_ITERATE_BATCH_SIZE;

/**
 * A simple test that asserts that list status output is sorted.
 */
public class TestListStatus {

  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static String clusterId;
  private static String scmId;
  private static String omId;
  private static OzoneBucket fsoOzoneBucket;

  @Rule
  public Timeout timeout = new Timeout(1200000);

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
        true);
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omId = UUID.randomUUID().toString();
    cluster = MiniOzoneCluster.newBuilder(conf).setClusterId(clusterId)
        .setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();

    // create a volume and a LEGACY bucket
    fsoOzoneBucket = TestDataUtil
        .createVolumeAndBucket(cluster, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);

    buildNameSpaceTree(fsoOzoneBucket);
  }

  @AfterClass
  public static void teardownClass() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testSortedListStatus() throws Exception {
    // a) test if output is sorted
    checkKeyList("", "", 1000, 10, false);

    // b) number of keys returns is expected
    checkKeyList("", "", 2, 2, false);

    // c) check if full prefix works
    checkKeyList("a1", "", 100, 3, false);

    //  d) check if full prefix with numEntries work
    checkKeyList("a1", "", 2, 2, false);

    // e) check if existing start key >>>
    checkKeyList("a1", "a1/a12", 100, 2, false);

    // f) check with non-existing start key
    checkKeyList("", "a7", 100, 6, false);

    // g) check if half prefix works
    checkKeyList("b", "", 100, 4, true);

    // h) check half prefix with non-existing start key
    checkKeyList("b", "b5", 100, 2, true);

    // i) check half prefix with non-existing parent in start key
    checkKeyList("b", "c", 100, 0, true);

    // i) check half prefix with non-existing parent in start key
    checkKeyList("b", "b/g5", 100, 4, true);

    // i) check half prefix with non-existing parent in start key
    checkKeyList("b", "c/g5", 100, 0, true);

    // j) check prefix with non-existing prefix key
    //    and non-existing parent in start key
    checkKeyList("a1/a111", "a1/a111/a100", 100, 0, true);
  }

  private static void createFile(OzoneBucket bucket, String keyName)
      throws IOException {
    try (OzoneOutputStream oos = bucket.createFile(keyName, 0,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        true, false)) {
      oos.flush();
    }
  }
  private static void buildNameSpaceTree(OzoneBucket ozoneBucket)
      throws Exception {
    /*
    FileSystem Namespace:
    "a1"      Dir
    "a1/a11"  Dir
    "a1/a12"  File
    "a1/a13"  File
    "a2"      File
    "a3"      Dir
    "a3/a31"  Dir
    "a3/a32"  File
    "a8"      File
    "a9"      Dir
    "a10"     File

    "b1"      File
    "b2"      File
    "b7"      File
    "b8"      File
     */
    ozoneBucket.createDirectory("/a1");
    createFile(ozoneBucket, "/a2");
    ozoneBucket.createDirectory("/a3");
    createFile(ozoneBucket, "/a8");
    ozoneBucket.createDirectory("/a9");
    createFile(ozoneBucket, "/a10");

    ozoneBucket.createDirectory("/a1/a11");
    createFile(ozoneBucket, "/a1/a12");
    createFile(ozoneBucket, "/a1/a13");

    ozoneBucket.createDirectory("/a3/a31");
    createFile(ozoneBucket, "/a3/a32");

    createFile(ozoneBucket, "/b1");
    createFile(ozoneBucket, "/b2");
    createFile(ozoneBucket, "/b7");
    createFile(ozoneBucket, "/b8");
  }

  private void checkKeyList(String keyPrefix, String startKey,
                            long numEntries, int expectedNumKeys,
                            boolean isPartialPrefix)
      throws Exception {

    List<OzoneFileStatus> statuses =
        fsoOzoneBucket.listStatus(keyPrefix, false, startKey,
            numEntries, isPartialPrefix);
    Assert.assertEquals(expectedNumKeys, statuses.size());

    System.out.println("BEGIN:::keyPrefix---> " + keyPrefix + ":::---> " +
        startKey);

    for (int i = 0; i < statuses.size() - 1; i++) {
      OzoneFileStatus stCurr = statuses.get(i);
      OzoneFileStatus stNext = statuses.get(i + 1);

      System.out.println("status:"  + stCurr);
      Assert.assertTrue(stCurr.getPath().compareTo(stNext.getPath()) < 0);
    }

    if (!statuses.isEmpty()) {
      OzoneFileStatus stNext = statuses.get(statuses.size() - 1);
      System.out.println("status:" + stNext);
    }

    System.out.println("END:::keyPrefix---> " + keyPrefix + ":::---> " +
        startKey);
  }
}
