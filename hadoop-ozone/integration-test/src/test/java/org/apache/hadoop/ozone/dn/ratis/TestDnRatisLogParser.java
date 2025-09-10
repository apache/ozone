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

package org.apache.hadoop.ozone.dn.ratis;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.debug.ratis.parse.RatisLogParser;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test Datanode Ratis log parser.
 */
public class TestDnRatisLogParser {

  private MiniOzoneCluster cluster = null;
  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 2);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();
    System.setOut(new PrintStream(out, false, UTF_8.name()));
    System.setErr(new PrintStream(err, false, UTF_8.name()));
  }

  @AfterEach
  public void destroy() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }

    out.close();
    err.close();
  }

  @Test
  public void testRatisLogParsing() throws Exception {
    OzoneConfiguration conf = cluster.getHddsDatanodes().get(0).getConf();
    String path =
        conf.get(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR);
    UUID pid = cluster.getStorageContainerManager().getPipelineManager()
        .getPipelines().get(0).getId().getId();
    File pipelineDir = new File(path, pid.toString());
    File currentDir = new File(pipelineDir, "current");
    File logFile = new File(currentDir, "log_inprogress_0");
    GenericTestUtils.waitFor(logFile::exists, 100, 15000);
    assertThat(logFile).isFile();

    RatisLogParser datanodeRatisLogParser =
        new RatisLogParser();
    datanodeRatisLogParser.setSegmentFile(logFile);
    datanodeRatisLogParser.parseRatisLogs(
        RatisLogParser::smToContainerLogString);
    assertThat(out.toString(StandardCharsets.UTF_8.name()))
        .contains("Num Total Entries:");
  }
}
