/**
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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for Recon Node Manager.
 */
public class TestReconNodeManager {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private OzoneConfiguration conf;

  @Before
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS,
        temporaryFolder.newFolder().getAbsolutePath());
    conf.set(OZONE_SCM_NAMES, "localhost");
  }

  @Test
  public void testReconNodeDB() throws IOException {
    ReconStorageConfig scmStorageConfig = new ReconStorageConfig(conf);
    EventQueue eventQueue = new EventQueue();
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    ReconNodeManager reconNodeManager = new ReconNodeManager(conf,
        scmStorageConfig, eventQueue, clusterMap);
    ReconNewNodeHandler reconNewNodeHandler =
        new ReconNewNodeHandler(reconNodeManager);
    assertTrue(reconNodeManager.getAllNodes().isEmpty());

    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    String uuidString = datanodeDetails.getUuidString();

    // Register a random datanode.
    reconNodeManager.register(datanodeDetails, null, null);
    reconNewNodeHandler.onMessage(reconNodeManager.getNodeByUuid(uuidString),
        null);

    assertEquals(1, reconNodeManager.getAllNodes().size());
    assertNotNull(reconNodeManager.getNodeByUuid(uuidString));

    // Close the DB, and recreate the instance of Recon Node Manager.
    eventQueue.close();
    reconNodeManager.close();
    reconNodeManager = new ReconNodeManager(conf,
        scmStorageConfig, eventQueue, clusterMap);

    // Verify that the node information was persisted and loaded back.
    assertEquals(1, reconNodeManager.getAllNodes().size());
    assertNotNull(
        reconNodeManager.getNodeByUuid(datanodeDetails.getUuidString()));
  }
}