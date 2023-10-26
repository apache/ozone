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
package org.apache.hadoop.ozone.parser;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.ha.SCMRatisRequest;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.segmentparser.OMRatisLogParser;
import org.apache.hadoop.ozone.segmentparser.SCMRatisLogParser;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;

/**
 * Test Ozone OM and SCM HA Ratis log parser.
 */
@Flaky("HDDS-7008")
@Timeout(300)
class TestOzoneHARatisLogParser {

  private MiniOzoneHAClusterImpl cluster = null;
  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private OzoneClient client;

  @BeforeEach
  void setup() throws Exception {
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omServiceId = "omServiceId1";
    OzoneConfiguration conf = new OzoneConfiguration();
    String scmServiceId = "scmServiceId";
    cluster =  (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setSCMServiceId(scmServiceId)
        .setNumOfOzoneManagers(3)
        .setNumOfStorageContainerManagers(3)
        .build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(omServiceId, conf);
    ObjectStore objectStore = client.getObjectStore();
    performFewRequests(objectStore);
    System.setOut(new PrintStream(out, false, UTF_8.name()));
    System.setErr(new PrintStream(err, false, UTF_8.name()));
  }

  private void performFewRequests(ObjectStore objectStore) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(
        UUID.randomUUID().toString());
  }

  @AfterEach
  void destroy() throws Exception {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }

    out.close();
    err.close();
  }

  @Test
  void testRatisLogParsing() throws Exception {
    OzoneConfiguration ozoneConfiguration =
        cluster.getOMLeader().getConfiguration();

    StorageContainerManager scm = cluster.getActiveSCM();
    Preconditions.checkNotNull(scm);
    OzoneConfiguration leaderSCMConfig = scm.getConfiguration();

    cluster.stop();

    File omMetaDir = new File(ozoneConfiguration.get(OZONE_METADATA_DIRS),
        "ratis");
    Assertions.assertTrue(omMetaDir.isDirectory());

    String[] ratisDirs = omMetaDir.list();
    Assertions.assertNotNull(ratisDirs);
    Assertions.assertEquals(1, ratisDirs.length);

    File groupDir = new File(omMetaDir, ratisDirs[0]);

    Assertions.assertNotNull(groupDir);
    Assertions.assertTrue(groupDir.isDirectory());
    File currentDir = new File(groupDir, "current");
    File logFile = new File(currentDir, "log_inprogress_0");
    GenericTestUtils.waitFor(logFile::exists, 100, 15000);
    Assertions.assertTrue(logFile.isFile());

    OMRatisLogParser omRatisLogParser = new OMRatisLogParser();
    omRatisLogParser.setSegmentFile(logFile);
    omRatisLogParser.parseRatisLogs(OMRatisHelper::smProtoToString);


    // Not checking total entry count, because of not sure of exact count of
    // metadata entry changes.
    Assertions.assertTrue(out.toString(UTF_8.name())
        .contains("Num Total Entries:"));
    out.reset();

    // Now check for SCM.
    File scmMetadataDir =
        new File(SCMHAUtils.getRatisStorageDir(leaderSCMConfig));
    Assertions.assertTrue(scmMetadataDir.isDirectory());

    ratisDirs = scmMetadataDir.list();
    Assertions.assertNotNull(ratisDirs);
    Assertions.assertEquals(1, ratisDirs.length);

    groupDir = new File(scmMetadataDir, ratisDirs[0]);

    Assertions.assertNotNull(groupDir);
    Assertions.assertTrue(groupDir.isDirectory());
    currentDir = new File(groupDir, "current");
    logFile = new File(currentDir, "log_inprogress_1");
    GenericTestUtils.waitFor(logFile::exists, 100, 15000);
    Assertions.assertTrue(logFile.isFile());

    SCMRatisLogParser scmRatisLogParser = new SCMRatisLogParser();
    scmRatisLogParser.setSegmentFile(logFile);
    scmRatisLogParser.parseRatisLogs(SCMRatisRequest::smProtoToString);

    // Not checking total entry count, because of not sure of exact count of
    // metadata entry changes.
    Assertions.assertTrue(out.toString(UTF_8.name())
        .contains("Num Total Entries:"));
  }
}
