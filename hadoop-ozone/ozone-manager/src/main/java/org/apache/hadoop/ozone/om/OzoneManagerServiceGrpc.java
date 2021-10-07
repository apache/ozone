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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ClientId;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc.OzoneManagerServiceImplBase;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.OzoneDelegationTokenSecretManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.OMTokenProto.Type.S3AUTHINFO;

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
  private OzoneDelegationTokenSecretManager delegationTokenMgr;
  private final SecurityConfig secConfig;

  OzoneManagerServiceGrpc(
      OzoneManagerProtocolServerSideTranslatorPB omTranslator,
      OzoneDelegationTokenSecretManager delegationTokenMgr,
      OzoneConfiguration configuration) {
    this.omTranslator = omTranslator;
    this.delegationTokenMgr = delegationTokenMgr;
    this.secConfig = new SecurityConfig(configuration);
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

    if (secConfig.isSecurityEnabled()) {
      if (request.hasS3Authentication()) {
        S3Authentication auth = request.getS3Authentication();
        OzoneTokenIdentifier identifier = new OzoneTokenIdentifier();
        identifier.setTokenType(S3AUTHINFO);
        identifier.setStrToSign(auth.getStringToSign());
        identifier.setSignature(auth.getSignature());
        identifier.setAwsAccessId(auth.getAccessId());
        identifier.setOwner(new Text(auth.getAccessId()));
        try {
          // authenticate user with signature verification through
          // delegationTokenMgr validateToken via retrievePassword
          delegationTokenMgr.retrievePassword(identifier);
        } catch (Throwable e) {
          LOG.error("signatures do NOT match for S3 identifier:{}",
              identifier, e);
          responseObserver.onNext(
              createErrorResponse(request,
                  new OMException("User " + request.getUserInfo()
                      .getUserName() +
                      " request authorization failure: " +
                      "signatures do NOT match",
                      OMException.ResultCodes.S3_SECRET_NOT_FOUND)));
          responseObserver.onCompleted();
          return;
        }
      }
    }

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
    try {
      omResponse =
          UserGroupInformation.getCurrentUser().doAs(
              (PrivilegedExceptionAction<OMResponse>) () -> {
                try {
                  return this.omTranslator.
                      submitRequest(NULL_RPC_CONTROLLER, request);
                } catch (Throwable se) {
                  Throwable e = se.getCause();
                  if (se == null) {
                    throw new IOException(se);
                  } else {
                    throw e instanceof IOException ?
                        (IOException) e : new IOException(se);
                  }
                }
              });
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
