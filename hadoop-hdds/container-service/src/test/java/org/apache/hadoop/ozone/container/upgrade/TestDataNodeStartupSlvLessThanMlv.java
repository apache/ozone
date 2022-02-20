/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.upgrade;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.apache.hadoop.ozone.OzoneConsts.DATANODE_LAYOUT_VERSION_DIR;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.upgrade.UpgradeTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that DataNode will throw an exception on creation when it reads in a
 * VERSION file indicating a metadata layout version larger than its
 * software layout version.
 */
public class TestDataNodeStartupSlvLessThanMlv {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testStartupSlvLessThanMlv() throws Exception {
    // Add subdirectories under the temporary folder where the version file
    // will be placed.
    File datanodeSubdir = tempFolder.newFolder(DATANODE_LAYOUT_VERSION_DIR);

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        tempFolder.getRoot().getAbsolutePath());

    // Set metadata layout version larger then software layout version.
    int largestSlv = maxLayoutVersion();
    int mlv = largestSlv + 1;

    // Create version file with MLV > SLV, which should fail the
    // DataNodeStateMachine construction.
    UpgradeTestUtils.createVersionFile(datanodeSubdir,
        HddsProtos.NodeType.DATANODE, mlv);

    try {
      new DatanodeStateMachine(getNewDatanodeDetails(), conf, null,
                   null, null);
      Assert.fail("Expected IOException due to incorrect MLV on DataNode " +
          "creation.");
    } catch (IOException e) {
      String expectedMessage = String.format("Metadata layout version (%s) > " +
          "software layout version (%s)", mlv, largestSlv);
      GenericTestUtils.assertExceptionContains(expectedMessage, e);
    }
  }


  private DatanodeDetails getNewDatanodeDetails() {
    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, 0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, 0);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, 0);
    return DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort)
        .build();
  }
}