/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.shell;

import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY;

/**
 * Test for non ha prepare.
 */
public class TestOMNonHaPrepare {
  private static MiniOzoneCluster cluster;
  private static OzoneAdmin ozoneAdmin;
  private static String omServiceId;

  @BeforeClass
  public static void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    // om non-ratis
    conf.setBoolean(OZONE_OM_RATIS_ENABLE_KEY, false);
    omServiceId = UUID.randomUUID().toString();
    cluster = MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(UUID.randomUUID().toString())
        .setScmId(UUID.randomUUID().toString())
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(1)
        .setNumOfStorageContainerManagers(1)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    ozoneAdmin = new OzoneAdmin(cluster.getConf());
  }

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testOzoneManagerPrepare() throws Exception {
    try (GenericTestUtils.SystemOutCapturer capture =
         new GenericTestUtils.SystemOutCapturer()) {
      ozoneAdmin.execute(new String[] {
          "om",  "prepare", omServiceId});
      String output = capture.getOutput();

      Assert.assertTrue(output.equals(
          "prepare is no-op in the current non-ha setup!"));
    }
  }

}
