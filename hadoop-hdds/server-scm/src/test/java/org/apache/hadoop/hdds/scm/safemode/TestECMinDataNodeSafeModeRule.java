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

package org.apache.hadoop.hdds.scm.safemode;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ECMinDataNodeSafeModeRule}.
 */
public class TestECMinDataNodeSafeModeRule {

  @Test
  public void testDisabledForNonEcDefault() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_REPLICATION_TYPE,
        HddsProtos.ReplicationType.RATIS.name());

    NodeManager nodeManager = mock(NodeManager.class);
    SCMSafeModeManager safeModeManager = mock(SCMSafeModeManager.class);
    when(safeModeManager.getSafeModeMetrics()).thenReturn(mock(SafeModeMetrics.class));

    ECMinDataNodeSafeModeRule rule = new ECMinDataNodeSafeModeRule(
        new EventQueue(), conf, nodeManager, safeModeManager);

    assertFalse(rule.isEnabled());
    assertTrue(rule.validate());
  }

  @Test
  public void testEnabledForEcDefaultAndUsesRequiredNodeCount() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_REPLICATION_TYPE,
        HddsProtos.ReplicationType.EC.name());
    conf.set(OzoneConfigKeys.OZONE_REPLICATION, "rs-3-2-1024k");

    List<DatanodeDetails> enoughDns = new ArrayList<>();
    List<DatanodeDetails> insufficientDns = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      enoughDns.add(mock(DatanodeDetails.class));
      if (i < 4) {
        insufficientDns.add(mock(DatanodeDetails.class));
      }
    }

    NodeManager nodeManager = mock(NodeManager.class);
    when(nodeManager.getNodes(any())).thenReturn(enoughDns, insufficientDns);
    SCMSafeModeManager safeModeManager = mock(SCMSafeModeManager.class);
    when(safeModeManager.getSafeModeMetrics()).thenReturn(mock(SafeModeMetrics.class));

    ECMinDataNodeSafeModeRule rule = new ECMinDataNodeSafeModeRule(
        new EventQueue(), conf, nodeManager, safeModeManager);
    rule.setValidateBasedOnReportProcessing(false);

    assertTrue(rule.isEnabled());
    assertTrue(rule.validate());
    assertFalse(rule.validate());
  }
}
