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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.segmentparser.DatanodeRatisLogParser;

import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Test Datanode Ratis log parser.
 */
public class TestDnRatisLogParser {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private MiniOzoneCluster cluster = null;
  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1).setTotalPipelineNumLimit(2).build();
    cluster.waitForClusterToBeReady();
    System.setOut(new PrintStream(out, false, UTF_8.name()));
    System.setErr(new PrintStream(err, false, UTF_8.name()));
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
    cluster.stop();
    OzoneConfiguration conf = cluster.getHddsDatanodes().get(0).getConf();
    String path =
        conf.get(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR);
    UUID pid = cluster.getStorageContainerManager().getPipelineManager()
        .getPipelines().get(0).getId().getId();
    File pipelineDir = new File(path, pid.toString());
    File currentDir = new File(pipelineDir, "current");
    File logFile = new File(currentDir, "log_inprogress_0");
    GenericTestUtils.waitFor(logFile::exists, 100, 15000);
    Assert.assertTrue(logFile.isFile());

    DatanodeRatisLogParser datanodeRatisLogParser =
        new DatanodeRatisLogParser();
    datanodeRatisLogParser.setSegmentFile(logFile);
    datanodeRatisLogParser.parseRatisLogs(
        DatanodeRatisLogParser::smToContainerLogString);
    Assert.assertTrue(out.toString(StandardCharsets.UTF_8.name())
        .contains("Num Total Entries:"));
  }
}
