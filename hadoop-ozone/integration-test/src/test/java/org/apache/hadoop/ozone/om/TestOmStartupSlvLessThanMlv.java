/*
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
package org.apache.hadoop.ozone.om;

import java.io.File;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.ozone.upgrade.TestUpgradeUtils;


import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test that the ozone manager will not start when it loads a VERSION file
 * indicating a metadata layout version larger than its software layout version.
 */
public class TestOmStartupSlvLessThanMlv {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testStartupSlvLessThanMlv() throws Exception {
    // Add subdirectories under the temporary folder where the version file
    // will be placed.
    File omSubdir = tempFolder.newFolder("om", "current");

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        tempFolder.getRoot().getAbsolutePath());

    // Set metadata layout version larger then software layout version.
    int largestSlv = 0;
    for (LayoutFeature f: OMLayoutFeature.values()) {
      largestSlv = Math.max(largestSlv, f.layoutVersion());
    }
    int mlv = largestSlv + 1;

    // Create version file with MLV > SLV, which should fail the cluster build.
    TestUpgradeUtils.createVersionFile(omSubdir, HddsProtos.NodeType.OM, mlv);

    MiniOzoneCluster.Builder clusterBuilder = MiniOzoneCluster.newBuilder(conf)
        .setClusterId(UUID.randomUUID().toString())
        .setScmId(UUID.randomUUID().toString())
        .setOmId(UUID.randomUUID().toString());

    try {
      clusterBuilder.build();
      Assert.fail("Expected OMException due to incorrect MLV on OM creation.");
    } catch(OMException e) {
      String expectedMessage = String.format("Cannot initialize " +
              "VersionManager. Metadata layout version (%s) > software layout" +
              " version (%s)", mlv, largestSlv);
      GenericTestUtils.assertExceptionContains(expectedMessage, e);
    }
  }
}
