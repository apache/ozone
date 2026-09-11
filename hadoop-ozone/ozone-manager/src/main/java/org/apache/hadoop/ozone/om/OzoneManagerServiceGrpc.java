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

package org.apache.hadoop.ozone.om;

import com.google.protobuf.RpcController;
import io.grpc.Status;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc.OzoneManagerServiceImplBase;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.util.UUIDUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  /**
   * OMRequests the S3 gateway client must issue before any per-request S3
   * identity exists: RpcClient bootstrap (OzoneClientCache.initialize ->
   * getServiceInfo), the background CA-cert refresher, and server-defaults
   * lookups all fetch the ServiceList before EndpointBase has set any
   * thread-local S3Auth. These identity-free, read-only cmdTypes stay
   * callable without S3 authentication in secure mode.
   */
  private static final Set<Type> S3_AUTH_EXEMPT_CMD_TYPES =
      EnumSet.of(Type.ServiceList);
  private final OzoneManagerProtocolServerSideTranslatorPB omTranslator;
  private final boolean s3AuthRequired;

  OzoneManagerServiceGrpc(
      OzoneManagerProtocolServerSideTranslatorPB omTranslator,
      boolean s3AuthRequired) {
    this.omTranslator = omTranslator;
    this.s3AuthRequired = s3AuthRequired;
  }

  @Override
  public void submitRequest(OMRequest request,
                            io.grpc.stub.StreamObserver<OMResponse>
                                responseObserver) {
    LOG.debug("OzoneManagerServiceGrpc: OzoneManagerServiceImplBase " +
        "processing s3g client submit request - for command {}",
        request.getCmdType().name());

    // This endpoint carries no channel-level client authentication, so in
    // secure mode a request acting on behalf of a user must present S3
    // authentication (validated downstream against the caller's S3 secret);
    // without it the request identity would be client-asserted. Only the
    // identity-free bootstrap reads in S3_AUTH_EXEMPT_CMD_TYPES are exempt.
    if (s3AuthRequired && !request.hasS3Authentication()
        && !S3_AUTH_EXEMPT_CMD_TYPES.contains(request.getCmdType())) {
      LOG.warn("Rejecting {} OMRequest without S3 authentication received" +
          " on the S3G gRPC endpoint", request.getCmdType().name());
      responseObserver.onError(Status.UNAUTHENTICATED
          .withDescription("S3 authentication is required for requests to" +
              " this endpoint in secure mode (ozone.om.s3.grpc.auth.required)")
          .asRuntimeException());
      return;
    }
    AtomicInteger callCount = new AtomicInteger(0);

    org.apache.hadoop.ipc_.Server.getCurCall().set(new Server.Call(1,
        callCount.incrementAndGet(),
        null,
        null,
        RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        getClientId()));
    // TODO: currently require setting the Server class for each request
    // with thread context (Server.Call()) that includes retries
    // and importantly random ClientId.  This is currently necessary for
    // Om Ratis Server to create createWriteRaftClientRequest.
    // Look to remove Server class requirement for issuing ratis transactions
    // for OMRequests.  Test through successful ratis-enabled OMRequest
    // handling without dependency on hadoop IPC based Server.
    try {
      OMResponse omResponse = this.omTranslator.
          submitRequest(NULL_RPC_CONTROLLER, request);
      responseObserver.onNext(omResponse);
    } catch (Throwable e) {
      LOG.error("Failed to submit request", e);
      IOException ex = new IOException(e.getCause());
      responseObserver.onError(
          Status.INTERNAL.withDescription(ex.getMessage())
              .asRuntimeException());
      return;
    }
    responseObserver.onCompleted();
  }

  private static byte[] getClientId() {
    return UUIDUtil.randomUUIDBytes();
  }

}
