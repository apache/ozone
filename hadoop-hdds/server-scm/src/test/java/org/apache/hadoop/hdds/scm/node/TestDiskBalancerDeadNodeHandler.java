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

package org.apache.hadoop.hdds.scm.node;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerRunningStatus;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for DiskBalancerDeadNodeHandler.
 */
public class TestDiskBalancerDeadNodeHandler {

  private DiskBalancerManager diskBalancerManager;
  private DiskBalancerDeadNodeHandler deadNodeHandler;
  private DatanodeDetails healthyDn;
  private DatanodeDetails deadDn;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String storageDir = GenericTestUtils.getTempPath(
        TestDiskBalancerDeadNodeHandler.class.getSimpleName() + UUID.randomUUID());
    conf.set("ozone.metadata.dirs", storageDir);

    diskBalancerManager = new DiskBalancerManager(conf, new EventQueue(),
        SCMContext.emptyContext(), null);
    deadNodeHandler = new DiskBalancerDeadNodeHandler(diskBalancerManager);

    // Create two DNs: one healthy and one dead
    healthyDn = DatanodeDetails.newBuilder().setUuid(UUID.randomUUID()).build();
    deadDn = DatanodeDetails.newBuilder().setUuid(UUID.randomUUID()).build();

    diskBalancerManager.addRunningDatanode(healthyDn);
    diskBalancerManager.addRunningDatanode(deadDn);
  }

  @Test
  public void testDeadNodeHandlerUpdatesStatusToUnknown() {
    // Verify initial status of both datanodes
    assertEquals(DiskBalancerRunningStatus.RUNNING, diskBalancerManager.getStatus(healthyDn).getRunningStatus());
    assertEquals(DiskBalancerRunningStatus.RUNNING, diskBalancerManager.getStatus(deadDn).getRunningStatus());

    deadNodeHandler.onMessage(deadDn, null);

    // Verify that the status of the dead datanode is updated to UNKNOWN
    assertEquals(DiskBalancerRunningStatus.UNKNOWN, diskBalancerManager.getStatus(deadDn).getRunningStatus());

    // Verify that the status of the healthy datanode remains unchanged
    assertEquals(DiskBalancerRunningStatus.RUNNING, diskBalancerManager.getStatus(healthyDn).getRunningStatus());
  }
}
