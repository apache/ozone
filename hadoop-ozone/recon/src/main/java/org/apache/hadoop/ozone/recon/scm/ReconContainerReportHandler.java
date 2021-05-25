/*
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

import java.util.List;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.container.ContainerReportHandler;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon's container report handler.
 */
public class ReconContainerReportHandler extends ContainerReportHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconContainerReportHandler.class);

  public ReconContainerReportHandler(NodeManager nodeManager,
                                     ContainerManagerV2 containerManager) {
    super(nodeManager, containerManager);
  }

  @Override
  public void onMessage(final ContainerReportFromDatanode reportFromDatanode,
                        final EventPublisher publisher) {
    ReconContainerManager containerManager =
        (ReconContainerManager) getContainerManager();
    List<ContainerReplicaProto> containerReplicaProtoList =
        reportFromDatanode.getReport().getReportsList();
    containerManager.checkAndAddNewContainerBatch(containerReplicaProtoList);
    super.onMessage(reportFromDatanode, publisher);
  }

}
