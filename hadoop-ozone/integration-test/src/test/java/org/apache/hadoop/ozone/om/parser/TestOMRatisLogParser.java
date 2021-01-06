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
package org.apache.hadoop.ozone.om.parser;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.segmentparser.OMRatisLogParser;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.UUID;

import org.junit.Rule;
import org.junit.rules.Timeout;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;

/**
 * Test Datanode Ratis log parser.
 */
public class TestOMRatisLogParser {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = new Timeout(300000);

  private static MiniOzoneHAClusterImpl cluster = null;
  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();

  @Before
  public void setup() throws Exception {
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omServiceId = "omServiceId1";
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster =  (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(3)
        .build();
    cluster.waitForClusterToBeReady();
    ObjectStore objectStore = OzoneClientFactory.getRpcClient(omServiceId, conf)
        .getObjectStore();
    performFewRequests(objectStore);
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
  }

  private void performFewRequests(ObjectStore objectStore) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(
        UUID.randomUUID().toString());
  }

  @After
  public void destroy() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }

    out.close();
    err.close();
  }

  @Test
  public void testRatisLogParsing() throws Exception {
    OzoneConfiguration ozoneConfiguration =
        cluster.getOMLeader().getConfiguration();

    cluster.stop();

    File omMetaDir = new File(ozoneConfiguration.get(OZONE_METADATA_DIRS),
        "ratis");
    Assert.assertTrue(omMetaDir.isDirectory());

    String[] ratisDirs = omMetaDir.list();
    Assert.assertNotNull(ratisDirs);
    Assert.assertEquals(1, ratisDirs.length);

    File groupDir = new File(omMetaDir, ratisDirs[0]);

    Assert.assertNotNull(groupDir);
    Assert.assertTrue(groupDir.isDirectory());
    File currentDir = new File(groupDir, "current");
    File logFile = new File(currentDir, "log_inprogress_0");
    GenericTestUtils.waitFor(logFile::exists, 100, 15000);
    Assert.assertTrue(logFile.isFile());

    OMRatisLogParser omRatisLogParser = new OMRatisLogParser();
    omRatisLogParser.setSegmentFile(logFile);
    omRatisLogParser.parseRatisLogs(OMRatisHelper::smProtoToString);


    // Not checking total entry count, because of not sure of exact count of
    // metadata entry changes.
    Assert.assertTrue(out.toString().contains("Num Total Entries:"));
  }
}
