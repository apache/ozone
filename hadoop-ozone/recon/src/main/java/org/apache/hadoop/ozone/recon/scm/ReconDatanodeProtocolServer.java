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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;
import org.apache.hadoop.hdds.scm.HddsServerUtil;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;

/**
 * Recon's Datanode protocol server extended from SCM.
 */
public class ReconDatanodeProtocolServer extends SCMDatanodeProtocolServer {

  public ReconDatanodeProtocolServer(OzoneConfiguration conf,
                                     OzoneStorageContainerManager scm,
                                     EventPublisher eventPublisher)
      throws IOException {
    super(conf, scm, eventPublisher);
  }

  @Override
  public SCMVersionResponseProto getVersion(
      SCMVersionRequestProto versionRequest) throws IOException {
    return null;
  }

  @Override
  public SCMHeartbeatResponseProto sendHeartbeat(
      SCMHeartbeatRequestProto heartbeat) throws IOException {
    return null;
  }

  @Override
  public SCMRegisteredResponseProto register(
      DatanodeDetailsProto datanodeDetails,
      NodeReportProto nodeReport,
      ContainerReportsProto containerReportsRequestProto,
      PipelineReportsProto pipelineReports) throws IOException {
    return null;
  }

  @Override
  public InetSocketAddress getDataNodeBindAddress(OzoneConfiguration conf) {
    return HddsServerUtil.getReconDataNodeBindAddress(conf);
  }
}
