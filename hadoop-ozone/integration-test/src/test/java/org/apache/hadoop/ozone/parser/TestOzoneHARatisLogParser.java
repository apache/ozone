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

package org.apache.hadoop.ozone.parser;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Objects;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.ha.SCMRatisRequest;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.debug.ratis.parse.RatisLogParser;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test Ozone OM and SCM HA Ratis log parser.
 */
@Flaky("HDDS-7008")
class TestOzoneHARatisLogParser {

  private MiniOzoneHAClusterImpl cluster = null;
  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private OzoneClient client;

  @BeforeEach
  void setup() throws Exception {
    String omServiceId = "omServiceId1";
    OzoneConfiguration conf = new OzoneConfiguration();
    String scmServiceId = "scmServiceId";
    cluster =  MiniOzoneCluster.newHABuilder(conf)
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
    Objects.requireNonNull(scm, "scm == null");
    OzoneConfiguration leaderSCMConfig = scm.getConfiguration();

    cluster.stop();

    // Get the OM Ratis directory using the proper utility method
    File omMetaDir = new File(
        OzoneManagerRatisUtils.getOMRatisDirectory(ozoneConfiguration));
    assertThat(omMetaDir).isDirectory();

    String[] ratisDirs = omMetaDir.list();
    assertNotNull(ratisDirs);
    assertEquals(1, ratisDirs.length);

    File groupDir = new File(omMetaDir, ratisDirs[0]);

    assertNotNull(groupDir);
    assertThat(groupDir).isDirectory();
    File currentDir = new File(groupDir, "current");
    File logFile = new File(currentDir, "log_inprogress_0");
    GenericTestUtils.waitFor(logFile::exists, 100, 15000);
    assertThat(logFile).isFile();

    RatisLogParser omRatisLogParser = new RatisLogParser();
    omRatisLogParser.setSegmentFile(logFile);
    omRatisLogParser.parseRatisLogs(OMRatisHelper::smProtoToString);


    // Not checking total entry count, because of not sure of exact count of
    // metadata entry changes.
    assertThat(out.toString(UTF_8.name())).contains("Num Total Entries:");
    out.reset();

    // Now check for SCM.
    File scmMetadataDir =
        new File(SCMHAUtils.getSCMRatisDirectory(leaderSCMConfig));
    assertThat(scmMetadataDir).isDirectory();

    ratisDirs = scmMetadataDir.list();
    assertNotNull(ratisDirs);
    assertEquals(1, ratisDirs.length);

    groupDir = new File(scmMetadataDir, ratisDirs[0]);

    assertNotNull(groupDir);
    assertThat(groupDir).isDirectory();
    currentDir = new File(groupDir, "current");
    logFile = new File(currentDir, "log_inprogress_1");
    GenericTestUtils.waitFor(logFile::exists, 100, 15000);
    assertThat(logFile).isFile();

    RatisLogParser scmRatisLogParser = new RatisLogParser();
    scmRatisLogParser.setSegmentFile(logFile);
    scmRatisLogParser.parseRatisLogs(SCMRatisRequest::smProtoToString);

    // Not checking total entry count, because of not sure of exact count of
    // metadata entry changes.
    assertThat(out.toString(UTF_8.name())).contains("Num Total Entries:");
  }
}
