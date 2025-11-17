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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMInterServiceProtocolPB;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerInterServiceProtocolProtos.BootstrapOMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerInterServiceProtocolProtos.BootstrapOMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerInterServiceProtocolProtos.ErrorCode;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link OMInterServiceProtocolPB}
 * to the OzoneManagerInterService server implementation.
 */
public class OMInterServiceProtocolServerSideImpl implements
    OMInterServiceProtocolPB {

  private final OzoneManagerRatisServer omRatisServer;
  private final OzoneManager ozoneManager;

  public OMInterServiceProtocolServerSideImpl(OzoneManager ozoneMgr,
      OzoneManagerRatisServer ratisServer) {
    this.ozoneManager = ozoneMgr;
    this.omRatisServer = ratisServer;
  }

  @Override
  public BootstrapOMResponse bootstrap(RpcController controller,
      BootstrapOMRequest request) throws ServiceException {
    if (request == null) {
      return null;
    }

    OzoneManagerRatisUtils.checkLeaderStatus(ozoneManager);

    OMNodeDetails newOmNode = new OMNodeDetails.Builder()
        .setOMNodeId(request.getNodeId())
        .setHostAddress(request.getHostAddress())
        .setRatisPort(request.getRatisPort())
        .setIsListener(request.getIsListener())
        .build();

    try {
      omRatisServer.addOMToRatisRing(newOmNode);
    } catch (IOException ex) {
      return BootstrapOMResponse.newBuilder()
          .setSuccess(false)
          .setErrorCode(ErrorCode.RATIS_BOOTSTRAP_ERROR)
          .setErrorMsg(ex.getMessage())
          .build();
    }

    return BootstrapOMResponse.newBuilder()
        .setSuccess(true)
        .build();
  }
}
