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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMMetadataProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerMetadataProtocolProtos.OMConfigurationRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerMetadataProtocolProtos.OMConfigurationResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerMetadataProtocolProtos.OMNodeInfo;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link OMMetadataProtocolPB} to the OMMetadataProtocolServer implementation.
 */
public class OMMetadataProtocolServerSideImpl implements OMMetadataProtocolPB {

  private final OzoneManager ozoneManager;

  public OMMetadataProtocolServerSideImpl(OzoneManager om) {
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
}
