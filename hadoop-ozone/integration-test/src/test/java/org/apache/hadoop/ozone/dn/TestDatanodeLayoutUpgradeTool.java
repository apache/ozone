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
package org.apache.hadoop.ozone.dn;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.debug.DatanodeLayout;
import org.junit.Before;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

/**
 * Test Datanode Layout Upgrade Tool.
 */
public class TestDatanodeLayoutUpgradeTool {
  private MiniOzoneCluster cluster = null;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1).setTotalPipelineNumLimit(2).build();
    cluster.waitForClusterToBeReady();
  }

  @After
  public void destroy() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDatanodeLayoutVerify() throws Exception {
    cluster.shutdown();
    List<HddsDatanodeService> dns = cluster.getHddsDatanodes();
    cluster = null;
    OzoneConfiguration c1 = dns.get(0).getConf();
    Collection<String> paths = MutableVolumeSet.getDatanodeStorageDirs(c1);
    for (String p : paths) {
      List<HddsVolume> volumes = DatanodeLayout.runUpgrade(c1, p, true);
      Assert.assertEquals(0, volumes.size());
    }
  }
}
