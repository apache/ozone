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
package org.apache.hadoop.ozone.container.common.states.endpoint;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMLifelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMLifelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.mockito.ArgumentCaptor;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_RECON_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class tests the functionality of {@link LifelineEndpointTask}.
 */
public class TestLifelineEndpointTask {
  private static final InetSocketAddress TEST_SCM_ENDPOINT =
      new InetSocketAddress("test-scm-1", 9861);

  @Test
  public void testLifelineWithReports() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setTimeDuration(HDDS_RECON_HEARTBEAT_INTERVAL, 1, TimeUnit.SECONDS);
    DatanodeStateMachine datanodeStateMachine = mock(DatanodeStateMachine.class);

    when(datanodeStateMachine.getLayoutVersionManager())
        .thenReturn(new HDDSLayoutVersionManager(maxLayoutVersion()));
    when(datanodeStateMachine.getContainer())
        .thenReturn(mock(OzoneContainer.class));
    when(datanodeStateMachine.getContainer().getNodeReport())
        .thenReturn(NodeReportProto.getDefaultInstance());

    StateContext context = new StateContext(conf,
        DatanodeStateMachine.DatanodeStates.RUNNING,
        datanodeStateMachine, "");
    StorageContainerDatanodeProtocolClientSideTranslatorPB scm =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    ArgumentCaptor<SCMLifelineRequestProto> argument = ArgumentCaptor
        .forClass(SCMLifelineRequestProto.class);
    when(scm.sendLifeline(argument.capture()))
        .thenAnswer(invocation ->
        SCMLifelineResponseProto.newBuilder()
        .setDatanodeUUID(((SCMLifelineRequestProto)invocation.getArgument(0))
        .getDatanodeDetails().getUuid()).build());

    EndpointStateMachine endpointStateMachine = new EndpointStateMachine(
        TEST_SCM_ENDPOINT, scm, conf, "");
    endpointStateMachine.setState(EndpointStateMachine.EndPointStates.HEARTBEAT);
    LifelineEndpointTask task = getLifelineEndpointTask(endpointStateMachine, context);
    Thread thread = new Thread(getLifelineEndpointTask(endpointStateMachine, context));
    thread.start();
    Thread.sleep(1000);
    task.setRunning(false);
    SCMLifelineRequestProto lifeline = argument.getValue();
    assertTrue(lifeline.hasDatanodeDetails());
    assertTrue(lifeline.hasNodeReport());
    assertTrue(lifeline.hasDataNodeLayoutVersion());
  }

  private LifelineEndpointTask getLifelineEndpointTask(
      EndpointStateMachine endpointStateMachine, StateContext context) {
    DatanodeDetails datanodeDetails = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID()).setHostName("localhost")
        .setIpAddress("127.0.0.1").build();
    HDDSLayoutVersionManager layoutVersionManager =
        mock(HDDSLayoutVersionManager.class);
    when(layoutVersionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());
    when(layoutVersionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());
    return new LifelineEndpointTask(endpointStateMachine, context,
        context.getParent().getContainer(), datanodeDetails);
  }
}
