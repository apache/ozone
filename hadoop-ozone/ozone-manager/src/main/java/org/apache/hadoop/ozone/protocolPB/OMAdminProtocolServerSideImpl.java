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

package org.apache.hadoop.ozone.protocolPB;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.getRemoteUser;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PERMISSION_DENIED;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.CompactRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.CompactResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.DecommissionOMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.DecommissionOMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMConfigurationRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMConfigurationResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMNodeInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.TriggerSnapshotDefragRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.TriggerSnapshotDefragResponse;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link OMAdminProtocolPB} to the OMAdminProtocolServer implementation.
 */
public class OMAdminProtocolServerSideImpl implements OMAdminProtocolPB {

  private final OzoneManager ozoneManager;

  public OMAdminProtocolServerSideImpl(OzoneManager om) {
    this.ozoneManager = om;
  }

  @Override
  public OMConfigurationResponse getOMConfiguration(RpcController controller,
      OMConfigurationRequest request) throws ServiceException {

    List<OMNodeDetails> oldOMNodesList = ozoneManager.getAllOMNodesInMemory();
    List<OMNodeDetails> newOMNodesList = ozoneManager.getAllOMNodesInNewConf();

    List<OMNodeInfo> omNodesInMemory = new ArrayList<>(oldOMNodesList.size());
    for (OMNodeDetails omNodeDetails : oldOMNodesList) {
      omNodesInMemory.add(omNodeDetails.getProtobuf());
    }

    List<OMNodeInfo> omNodesInNewConf =
        new ArrayList<>(newOMNodesList.size());
    for (OMNodeDetails omNodeDetails : newOMNodesList) {
      omNodesInNewConf.add(omNodeDetails.getProtobuf());
    }

    return OMConfigurationResponse.newBuilder()
        .setSuccess(true)
        .addAllNodesInMemory(omNodesInMemory)
        .addAllNodesInNewConf(omNodesInNewConf)
        .build();
  }

  @Override
  public DecommissionOMResponse decommission(RpcController controller,
      DecommissionOMRequest request) throws ServiceException {
    if (request == null) {
      return null;
    }

    OzoneManagerRatisServer omRatisServer = ozoneManager.getOmRatisServer();
    OzoneManagerRatisUtils.checkLeaderStatus(ozoneManager);

    OMNodeDetails decommNode = ozoneManager.getPeerNode(request.getNodeId());
    if (decommNode == null) {
      return DecommissionOMResponse.newBuilder()
          .setSuccess(false)
          .setErrorMsg("OM node not present in the OM peer list.")
          .build();
    }

    try {
      if (!ozoneManager.isAdmin(getRemoteUser())) {
        throw new OMException("Only administrators are authorized to perform decommission.", PERMISSION_DENIED);
      }
      omRatisServer.removeOMFromRatisRing(decommNode);
    } catch (IOException ex) {
      return DecommissionOMResponse.newBuilder()
          .setSuccess(false)
          .setErrorMsg(ex.getMessage())
          .build();
    }

    return DecommissionOMResponse.newBuilder()
        .setSuccess(true)
        .build();
  }

  @Override
  public CompactResponse compactDB(RpcController controller, CompactRequest compactRequest)
      throws ServiceException {
    try {
      // check if table exists. IOException is thrown if table is not found.
      ozoneManager.getMetadataManager().getStore().getTable(compactRequest.getColumnFamily());
      ozoneManager.compactOMDB(compactRequest.getColumnFamily());
    } catch (Exception ex) {
      return CompactResponse.newBuilder()
          .setSuccess(false)
          .setErrorMsg(ex.getMessage())
          .build();
    }

    return CompactResponse.newBuilder()
        .setSuccess(true).build();
  }

  @Override
  public TriggerSnapshotDefragResponse triggerSnapshotDefrag(
      RpcController controller, TriggerSnapshotDefragRequest request)
      throws ServiceException {
    try {
      boolean result = ozoneManager.triggerSnapshotDefrag(request.getNoWait());
      return TriggerSnapshotDefragResponse.newBuilder()
          .setSuccess(true)
          .setResult(result)
          .build();
    } catch (Exception ex) {
      return TriggerSnapshotDefragResponse.newBuilder()
          .setSuccess(false)
          .setErrorMsg(ex.getMessage())
          .build();
    }
  }
}
