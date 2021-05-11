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
package org.apache.hadoop.ozone.freon;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.test.GenericTestUtils;

import org.apache.commons.io.FileUtils;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

/**
 * Test for OzoneClientKeyGenerator.
 */
public class TestOzoneClientKeyGenerator {

  private String path;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @Before
  public void setup() {
    path = GenericTestUtils
        .getTempPath(TestOzoneClientKeyGenerator.class.getSimpleName());
    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    File baseDir = new File(path);
    baseDir.mkdirs();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  private void shutdown(MiniOzoneCluster cluster) throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      FileUtils.deleteDirectory(new File(path));
    }
  }

  private MiniOzoneCluster startCluster(OzoneConfiguration conf)
      throws Exception {
    if (conf == null) {
      conf = new OzoneConfiguration();
    }
    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .build();

    cluster.waitForClusterToBeReady();
    cluster.waitTobeOutOfSafeMode();
    return cluster;
  }

  @Test
  public void testOzoneClientKeyGenerator() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    MiniOzoneCluster cluster = startCluster(conf);
    FileOutputStream out = FileUtils.openOutputStream(new File(path, "conf"));
    cluster.getConf().writeXml(out);
    out.getFD().sync();
    out.close();
    new Freon().execute(
        new String[] {"-conf", new File(path, "conf").getAbsolutePath(),
            "ockg", "-t", "1"});
    shutdown(cluster);
  }

}
