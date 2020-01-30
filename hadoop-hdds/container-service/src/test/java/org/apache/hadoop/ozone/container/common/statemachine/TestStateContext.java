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

package org.apache.hadoop.ozone.container.common.statemachine;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerAction.Action.CLOSE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine.DatanodeStates;
import org.junit.Test;

import com.google.protobuf.GeneratedMessage;

/**
 * Test class for Datanode StateContext.
 */
public class TestStateContext {

  @Test
  public void testReportAPIs() {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachineMock =
        mock(DatanodeStateMachine.class);
    StateContext stateContext = new StateContext(conf,
        DatanodeStates.getInitState(), datanodeStateMachineMock);

    String scm1 = "scm1:9001";
    String scm2 = "scm2:9001";

    // Try to add report with endpoint. Should not be stored.
    stateContext.addReport(mock(GeneratedMessage.class));
    assertTrue(stateContext.getAllAvailableReports(scm1).isEmpty());

    // Add 2 scm endpoints.
    stateContext.addEndpoint(scm1);
    stateContext.addEndpoint(scm2);

    // Add report. Should be added to all endpoints.
    stateContext.addReport(mock(GeneratedMessage.class));
    List<GeneratedMessage> allAvailableReports =
        stateContext.getAllAvailableReports(scm1);
    assertEquals(1, allAvailableReports.size());
    assertEquals(1, stateContext.getAllAvailableReports(scm2).size());

    // Assert the reports are no longer available.
    assertTrue(stateContext.getAllAvailableReports(scm1).isEmpty());

    // Put back reports.
    stateContext.putBackReports(allAvailableReports, scm1);
    assertFalse(stateContext.getAllAvailableReports(scm1).isEmpty());
  }

  @Test
  public void testActionAPIs() {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachineMock =
        mock(DatanodeStateMachine.class);
    StateContext stateContext = new StateContext(conf,
        DatanodeStates.getInitState(), datanodeStateMachineMock);

    String scm1 = "scm1:9001";
    String scm2 = "scm2:9001";

    // Try to get containerActions for endpoint which is not yet added.
    List<ContainerAction> containerActions =
        stateContext.getPendingContainerAction(scm1, 10);
    assertTrue(containerActions.isEmpty());

    // Try to get pipelineActions for endpoint which is not yet added.
    List<PipelineAction> pipelineActions =
        stateContext.getPendingPipelineAction(scm1, 10);
    assertTrue(pipelineActions.isEmpty());

    // Add 2 scm endpoints.
    stateContext.addEndpoint(scm1);
    stateContext.addEndpoint(scm2);

    // Add PipelineAction. Should be added to all endpoints.
    stateContext.addPipelineActionIfAbsent(
        PipelineAction.newBuilder().setAction(
            PipelineAction.Action.CLOSE).build());

    pipelineActions = stateContext.getPendingPipelineAction(scm1, 10);
    assertEquals(1, pipelineActions.size());

    // Add ContainerAction. Should be added to all endpoints.
    stateContext.addContainerAction(ContainerAction.newBuilder()
        .setAction(CLOSE).setContainerID(100L).build());

    containerActions = stateContext.getPendingContainerAction(scm2, 10);
    assertEquals(1, containerActions.size());
  }

}