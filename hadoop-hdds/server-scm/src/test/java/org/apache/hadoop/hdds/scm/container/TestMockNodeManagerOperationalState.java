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

package org.apache.hadoop.hdds.scm.container;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.junit.jupiter.api.Test;

class TestMockNodeManagerOperationalState {

  @Test
  void testSetNodeOperationalStateUpdatesNodeStatus() throws Exception {
    MockNodeManager nm = new MockNodeManager(false, 0);
    DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();

    // register via NodeManager default overload (what ctors do)
    nm.register(dn, null, null);

    nm.setNodeOperationalState(dn,
        HddsProtos.NodeOperationalState.DECOMMISSIONING, 123L);

    NodeStatus s = nm.getNodeStatus(dn);
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONING,
        s.getOperationalState());
    assertEquals(123L, s.getOpStateExpiryEpochSeconds());
  }

  @Test
  void testSetNodeOperationalStateForUnknownNodeThrows() {
    MockNodeManager nm = new MockNodeManager(false, 0);
    DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();

    assertThrows(NodeNotFoundException.class, () ->
        nm.setNodeOperationalState(dn,
            HddsProtos.NodeOperationalState.DECOMMISSIONING, 123L));
  }
}
