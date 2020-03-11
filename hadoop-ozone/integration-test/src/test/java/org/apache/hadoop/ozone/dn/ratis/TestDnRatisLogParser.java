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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.segmentparser.DatanodeRatisLogParser;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

/**
 * Test Datanode Ratis log parser.
 */
public class TestDnRatisLogParser {

  private static MiniOzoneCluster cluster = null;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1).setTotalPipelineNumLimit(2).build();
    cluster.waitForClusterToBeReady();
  }

  @After
  public void destroy() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testRatisLogParsing() {
    cluster.stop();
    Configuration conf = cluster.getHddsDatanodes().get(0).getConf();
    String path =
        conf.get(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR);
    UUID pid = cluster.getStorageContainerManager().getPipelineManager()
        .getPipelines().get(0).getId().getId();
    File pipelineDir = new File(path, pid.toString());
    File currentDir = new File(pipelineDir, "current");
    File logFile = new File(currentDir, "log_inprogress_0");
    Assert.assertTrue(logFile.exists());
    Assert.assertTrue(logFile.isFile());

    new DatanodeRatisLogParser()
        .parseRatisLogs(DatanodeRatisLogParser::smToContainerLogString);
  }
}
