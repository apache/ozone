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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMLifelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZonedDateTime;

import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.toLayoutVersionProto;

/**
 * Datanode periodically sends lifeline data to SCM.
 * This data includes basic node information.
 */
public class LifelineEndpointTask implements Runnable {

  public static final Logger LOG = LoggerFactory.getLogger(
        LifelineEndpointTask.class);

  private final EndpointStateMachine rpcEndpoint;
  private final DatanodeDetails datanodeDetails;
  private final OzoneContainer datanodeContainerManager;
  private final HDDSLayoutVersionManager layoutVersionManager;
  private final long heartbeatFrequency;
  private boolean isRunning = false;

  public LifelineEndpointTask(EndpointStateMachine rpcEndpoint,
      StateContext context, OzoneContainer datanodeContainerManager,
      DatanodeDetails datanodeDetails) {
    this.rpcEndpoint = rpcEndpoint;
    this.layoutVersionManager = context.getParent().getLayoutVersionManager();
    this.datanodeDetails = datanodeDetails;
    this.datanodeContainerManager = datanodeContainerManager;
    if (rpcEndpoint.isPassive()) {
      heartbeatFrequency = context.getReconHeartbeatFrequency();
    } else {
      heartbeatFrequency = context.getHeartbeatFrequency();
    }
  }

  @Override
  public void run() {
    this.isRunning = true;
    while (isRunning) {
      if (rpcEndpoint.getState() == EndpointStateMachine.EndPointStates.SHUTDOWN) {
        return;
      }

      if (rpcEndpoint.getState() == EndpointStateMachine.EndPointStates.HEARTBEAT) {
        SCMLifelineRequestProto.Builder builder = null;
        try {
          LayoutVersionProto layoutinfo = toLayoutVersionProto(
              layoutVersionManager.getMetadataLayoutVersion(),
              layoutVersionManager.getSoftwareLayoutVersion());
          NodeReportProto nodeReport = datanodeContainerManager.getNodeReport();
          builder = SCMLifelineRequestProto.newBuilder()
              .setDatanodeDetails(datanodeDetails.getProtoBufMessage())
              .setNodeReport(nodeReport)
              .setDataNodeLayoutVersion(layoutinfo);
          SCMLifelineRequestProto lifelineRequest = builder.build();
          LOG.debug("Sending lifeline message : {}", lifelineRequest);
          rpcEndpoint.getEndPoint().sendLifeline(lifelineRequest);
          rpcEndpoint.setLastSuccessfulLifeline(ZonedDateTime.now());
        } catch (IOException ex) {
          Preconditions.checkState(builder != null);
          rpcEndpoint.logIfNeeded(ex);
        }
      }
      try {
        Thread.sleep(heartbeatFrequency);
      } catch (InterruptedException e) {
        this.isRunning = false;
        Thread.interrupted();
      }
    }
  }

  public void setRunning(boolean isRun) {
    this.isRunning = isRun;
  }

  public boolean isRun() {
    return isRunning;
  }
}
