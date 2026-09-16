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

package org.apache.hadoop.hdds.scm.ha;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMMetrics;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;

/**
 * Test SCMStateMachine events recording.
 */
public class TestSCMStateMachine {

  @Test
  public void testRatisEventsRecording() throws Exception {
    StorageContainerManager scm = mock(StorageContainerManager.class);
    SCMMetrics metrics = SCMMetrics.create();
    when(scm.getMetrics()).thenReturn(metrics);

    SCMHADBTransactionBuffer buffer = mock(SCMHADBTransactionBuffer.class);
    when(buffer.getLatestTrxInfo()).thenReturn(TransactionInfo.valueOf(TermIndex.valueOf(0, 0)));

    SCMStateMachine stateMachine = new SCMStateMachine(scm, buffer);

    stateMachine.notifyConfigurationChanged(1, 1, RaftProtos.RaftConfigurationProto.getDefaultInstance());
    assertTrue(metrics.getRatisEvents().contains("Configuration changed at term index"));

    metrics.unRegister();
  }
}
