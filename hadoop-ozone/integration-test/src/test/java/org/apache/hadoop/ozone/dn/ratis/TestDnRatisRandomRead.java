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

package org.apache.hadoop.ozone.dn.ratis;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ozone.TestOzoneFileSystem;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ozone.test.JUnit5AwareTimeout;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Reading files from RATIS/THREE pipelines must be from random DN
 * when client is not defined in cluster topology.
 */
public class TestDnRatisRandomRead {
  public TestDnRatisRandomRead() {
    try {
      teardown();
      init();
    } catch (Exception e) {
      LOG.info("Unexpected exception", e);
      fail("Unexpected exception:" + e.getMessage());
    }
  }

  /**
   * Set a timeout for each test.
   */
  @Rule
  public TestRule timeout = new JUnit5AwareTimeout(Timeout.seconds(600));

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneFileSystem.class);
  private static final BucketLayout bucketLayout = BucketLayout.LEGACY;
  private static final int REPLICATION_COUNT = 3;

  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static FileSystem fs;
  private static String volumeName;
  private static Path volumePath;
  private static String bucketName;
  private static Path bucketPath;

  private void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        bucketLayout.name());

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(REPLICATION_COUNT)
        .build();
    cluster.waitForClusterToBeReady();

    client = cluster.newClient();

    OzoneBucket ozoneBucket =
        TestDataUtil.createVolumeAndBucket(client, bucketLayout);
    volumeName = ozoneBucket.getVolumeName();
    bucketName = ozoneBucket.getName();

    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);

    fs = FileSystem.get(conf);

  }

  @AfterClass
  public static void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @Before
  public void createVolumeAndBucket() throws IOException {
    // create a volume and a bucket to be used by RootedOzoneFileSystem (OFS)
    OzoneBucket bucket =
        TestDataUtil.createVolumeAndBucket(client, bucketLayout);
    volumeName = bucket.getVolumeName();
    volumePath = new Path(OZONE_URI_DELIMITER, volumeName);
    bucketName = bucket.getName();
    bucketPath = new Path(volumePath, bucketName);
  }

  @After
  public void cleanup() throws IOException {
    fs.delete(bucketPath, true);
    fs.delete(volumePath, false);
  }

  @Test
  public void testRandomRead()
      throws Exception {
    String key = "ratiskeytest";
    int numFiles = 100;
    // write files
    for (int i = 0; i < numFiles; i++) {
      try (OzoneOutputStream outputStream = client.getObjectStore()
          .getVolume(volumeName)
          .getBucket(bucketName)
          .createKey(key + i, i * 100,
              RatisReplicationConfig.getInstance(
                  HddsProtos.ReplicationFactor.THREE),
              new HashMap<>())) {
        outputStream.write(RandomUtils.nextBytes(i));
      }
    }
    // read files
    for (int i = 0; i < numFiles; i++) {
      try (
          OzoneInputStream ozoneInputStream = client.getObjectStore()
              .getVolume(volumeName)
              .getBucket(bucketName)
              .readFile(key + i)) {
        ozoneInputStream.read();
      }
    }
    // collect metrics from datanode
    List<Long> transactionsCountByDn = new ArrayList<>();
    cluster.getHddsDatanodes().forEach(dn -> {
      long metrics =
          dn.getDatanodeStateMachine().getContainer().getMetrics()
              .getContainerOpsMetrics(
                  ContainerProtos.Type.GetBlock);
      if (metrics > 0) {
        transactionsCountByDn.add(metrics);
      }
      System.out.println(dn.getDatanodeDetails().getUuid() + " " + metrics);
    });
    // read transactions must exist in all three DN
    assertEquals(REPLICATION_COUNT, transactionsCountByDn.size());
    long avgTransactionCountPerNode = numFiles / REPLICATION_COUNT;
    // expected deviation between transactions count by each datanode is
    // 1/3 number of files per each datanode (3)
    long deviation = numFiles / REPLICATION_COUNT / 3;
    transactionsCountByDn.forEach(
        count -> assertTrue(Math.abs(count - avgTransactionCountPerNode) < deviation));
  }
}
