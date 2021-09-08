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
package org.apache.hadoop.ozone.om;

import com.google.protobuf.RpcController;
import org.apache.hadoop.ipc.ClientId;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc.OzoneManagerServiceImplBase;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OMResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Grpc Service for handling S3 gateway OzoneManagerProtocol client requests.
 */
public class OzoneManagerServiceGrpc extends OzoneManagerServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerServiceGrpc.class);
  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;
  private OzoneManagerProtocolServerSideTranslatorPB omTranslator;

  OzoneManagerServiceGrpc(
      OzoneManagerProtocolServerSideTranslatorPB omTranslator) {
    this.omTranslator = omTranslator;
  }

  @Override
  public void submitRequest(OMRequest request,
                            io.grpc.stub.StreamObserver<OMResponse>
                                responseObserver) {
    LOG.debug("OzoneManagerServiceGrpc: OzoneManagerServiceImplBase " +
        "processing s3g client submit request - for command {}",
        request.getCmdType().name());
    AtomicInteger callCount = new AtomicInteger(0);
    OMResponse omResponse;
    try {
      org.apache.hadoop.ipc.Server.getCurCall().set(new Server.Call(1,
          callCount.incrementAndGet(),
          null,
          null,
          RPC.RpcKind.RPC_PROTOCOL_BUFFER,
          ClientId.getClientId()));
      // TODO: currently require setting the Server class for each request
      // with thread context (Server.Call()) that includes retries
      // and importantly random ClientId.  This is currently necessary for
      // Om Ratis Server to create createWriteRaftClientRequest.
      // Look to remove Server class requirement for issuing ratis transactions
      // for OMRequests.  Test through successful ratis-enabled OMRequest 
      // handling without dependency on hadoop IPC based Server.
      omResponse = this.omTranslator.
          submitRequest(NULL_RPC_CONTROLLER, request);
    } catch (Throwable e) {
      omResponse = createErrorResponse(
          request,
          new IOException(e));
    }
    responseObserver.onNext(omResponse);
    responseObserver.onCompleted();
  }

  /**
   * Create OMResponse from the specified OMRequest and exception.
   *
   * @param omRequest
   * @param exception
   * @return OMResponse
   */
  private OMResponse createErrorResponse(
      OMRequest omRequest, IOException exception) {
    OMResponse.Builder omResponse = OMResponse.newBuilder()
        .setStatus(OzoneManagerRatisUtils.exceptionToResponseStatus(exception))
        .setCmdType(omRequest.getCmdType())
        .setTraceID(omRequest.getTraceID())
        .setSuccess(false);
    if (exception.getMessage() != null) {
      omResponse.setMessage(exception.getMessage());
    }
    return omResponse.build();
  }
}
