/*
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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.protocolPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.conf.ReconfigurationUtil;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.DecommissionOMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.DecommissionOMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMConfigurationRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMConfigurationResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.OMNodeInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.GetOmReconfigurationStatusRequestProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.GetOmReconfigurationStatusResponseProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.GetOmReconfigurationStatusConfigChangeProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.StartOmReconfigurationRequestProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.StartOmReconfigurationResponseProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.ListOmReconfigurablePropertiesRequestProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.ListOmReconfigurablePropertiesResponseProto;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link OMAdminProtocolPB} to the OMAdminProtocolServer implementation.
 */
public class OMAdminProtocolServerSideImpl implements OMAdminProtocolPB {

  private final OzoneManager ozoneManager;

  private final static StartOmReconfigurationResponseProto START_RECONFIG_RESP = 
      StartOmReconfigurationResponseProto.newBuilder().build();

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
    if (!ozoneManager.isRatisEnabled()) {
      return DecommissionOMResponse.newBuilder()
          .setSuccess(false)
          .setErrorMsg("OM node cannot be decommissioned as Ratis is " +
              "not enabled.")
          .build();
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
  public GetOmReconfigurationStatusResponseProto getOmReconfigurationStatus(
      RpcController controller, GetOmReconfigurationStatusRequestProto request)
      throws ServiceException {
    String operation = "getOmReconfigurationStatus";
    checkAdminUserPrivilege(operation);

    ReconfigurationTaskStatus status = ozoneManager.getReconfigurationTaskStatus();
    GetOmReconfigurationStatusResponseProto.Builder builder =
        GetOmReconfigurationStatusResponseProto.newBuilder();

    builder.setStartTime(status.getStartTime());
    if (status.stopped()) {
      builder.setEndTime(status.getEndTime());
      assert status.getStatus() != null;
      for (Map.Entry<ReconfigurationUtil.PropertyChange, Optional<String>> result : status
          .getStatus().entrySet()) {
        GetOmReconfigurationStatusConfigChangeProto.Builder changeBuilder = 
            GetOmReconfigurationStatusConfigChangeProto.newBuilder();
        ReconfigurationUtil.PropertyChange change = result.getKey();
        changeBuilder.setName(change.prop);
        changeBuilder.setOldValue(change.oldVal != null ? change.oldVal : "");
        if (change.newVal != null) {
          changeBuilder.setNewValue(change.newVal);
        }
        if (result.getValue().isPresent()) {
          // Get full stack trace.
          changeBuilder.setErrorMessage(result.getValue().get());
        }
        builder.addChanges(changeBuilder);
      }
    }
    return builder.build();
  }

  @Override
  public StartOmReconfigurationResponseProto startOmReconfiguration(RpcController controller,
      StartOmReconfigurationRequestProto request) throws ServiceException {
    String operation = "startOmReconfiguration";
    checkAdminUserPrivilege(operation);

    try {
      ozoneManager.startReconfigurationTask();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return START_RECONFIG_RESP;
  }

  @Override
  public ListOmReconfigurablePropertiesResponseProto listOmReconfigurableProperties(
      RpcController controller, ListOmReconfigurablePropertiesRequestProto request)
      throws ServiceException {
    String operation = "listOmReconfigurableProperties";
    checkAdminUserPrivilege(operation);

    Collection<String> properties = ozoneManager.getReconfigurableProperties();
    ListOmReconfigurablePropertiesResponseProto.Builder builder =
        ListOmReconfigurablePropertiesResponseProto.newBuilder();
    builder.addAllName(properties);
    return builder.build();
  }

  /**
   * Check ozone admin privilege, throws exception if not admin.
   */
  private void checkAdminUserPrivilege(String operation) throws ServiceException {
    try {
      ozoneManager.checkAdminUserPrivilege(operation);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
