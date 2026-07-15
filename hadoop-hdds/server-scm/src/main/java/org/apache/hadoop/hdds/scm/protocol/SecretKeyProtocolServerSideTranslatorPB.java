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

package org.apache.hadoop.hdds.scm.protocol;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocolScm;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.SCMGetCheckAndRotateResponse;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.SCMGetCurrentSecretKeyResponse;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.SCMGetSecretKeyRequest;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.SCMGetSecretKeyResponse;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.SCMSecretKeyRequest;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.SCMSecretKeyResponse;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.SCMSecretKeysListResponse;
import org.apache.hadoop.hdds.protocol.proto.SCMSecretKeyProtocolProtos.Status;
import org.apache.hadoop.hdds.protocolPB.SecretKeyProtocolDatanodePB;
import org.apache.hadoop.hdds.protocolPB.SecretKeyProtocolOmPB;
import org.apache.hadoop.hdds.scm.ha.RatisUtil;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.exception.SCMSecretKeyException;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ozone.util.ProtobufUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link SecretKeyProtocolDatanodePB} to the server implementation.
 */
public class SecretKeyProtocolServerSideTranslatorPB
    implements SecretKeyProtocolDatanodePB, SecretKeyProtocolOmPB {

  private static final Logger LOG =
      LoggerFactory.getLogger(SecretKeyProtocolServerSideTranslatorPB.class);

  private final SecretKeyProtocolScm impl;
  private final StorageContainerManager scm;
  private static final String ROLE_TYPE = "SCM";

  private OzoneProtocolMessageDispatcher<SCMSecretKeyRequest,
      SCMSecretKeyResponse, SCMSecretKeyProtocolProtos.Type> dispatcher;

  public SecretKeyProtocolServerSideTranslatorPB(SecretKeyProtocolScm impl,
      StorageContainerManager storageContainerManager,
      ProtocolMessageMetrics messageMetrics) {
    this.impl = impl;
    this.scm = storageContainerManager;
    this.dispatcher =
        new OzoneProtocolMessageDispatcher<>("SCMSecretKeyProtocol",
            messageMetrics, LOG);
  }

  @Override
  public SCMSecretKeyResponse submitRequest(RpcController controller,
      SCMSecretKeyRequest request) throws ServiceException {
    if (!scm.checkLeader()) {
      RatisUtil.checkRatisException(
          scm.getScmHAManager().getRatisServer().triggerNotLeaderException(),
          scm.getSecurityProtocolRpcPort(), scm.getScmId(), scm.getHostname(), ROLE_TYPE);
    }
    return dispatcher.processRequest(request, this::processRequest,
        request.getCmdType(), request.getTraceID());
  }

  public SCMSecretKeyResponse processRequest(SCMSecretKeyRequest request)
      throws ServiceException {
    SCMSecretKeyResponse.Builder scmSecurityResponse =
        SCMSecretKeyResponse.newBuilder().setCmdType(request.getCmdType())
            .setStatus(Status.OK);
    try {
      switch (request.getCmdType()) {
      case GetCurrentSecretKey:
        return scmSecurityResponse
            .setCurrentSecretKeyResponseProto(getCurrentSecretKey())
            .build();

      case GetSecretKey:
        return scmSecurityResponse.setGetSecretKeyResponseProto(
                getSecretKey(request.getGetSecretKeyRequest()))
            .build();

      case GetAllSecretKeys:
        return scmSecurityResponse
            .setSecretKeysListResponseProto(getAllSecretKeys())
            .build();

      case CheckAndRotate:
        return scmSecurityResponse
            .setCheckAndRotateResponseProto(
                checkAndRotate(request.getCheckAndRotateRequest().getForce()))
            .build();

      default:
        throw new IllegalArgumentException(
            "Unknown request type: " + request.getCmdType());
      }
    } catch (IOException e) {
      RatisUtil.checkRatisException(e, scm.getSecurityProtocolRpcPort(),
          scm.getScmId(), scm.getHostname(), ROLE_TYPE);
      scmSecurityResponse.setSuccess(false);
      scmSecurityResponse.setStatus(exceptionToResponseStatus(e));
      // If actual cause is set in SCMSecurityException, set message with
      // actual cause message.
      if (e.getMessage() != null) {
        scmSecurityResponse.setMessage(e.getMessage());
      } else {
        if (e.getCause() != null && e.getCause().getMessage() != null) {
          scmSecurityResponse.setMessage(e.getCause().getMessage());
        }
      }
      return scmSecurityResponse.build();
    }
  }

  private SCMSecretKeysListResponse getAllSecretKeys()
      throws IOException {
    SCMSecretKeysListResponse.Builder builder =
        SCMSecretKeysListResponse.newBuilder();
    impl.getAllSecretKeys()
        .stream().map(ManagedSecretKey::toProtobuf)
        .forEach(builder::addSecretKeys);
    return builder.build();
  }

  private SCMGetSecretKeyResponse getSecretKey(
      SCMGetSecretKeyRequest getSecretKeyRequest) throws IOException {
    SCMGetSecretKeyResponse.Builder builder =
        SCMGetSecretKeyResponse.newBuilder();
    UUID id = ProtobufUtils.fromProtobuf(getSecretKeyRequest.getSecretKeyId());
    ManagedSecretKey secretKey = impl.getSecretKey(id);
    if (secretKey != null) {
      builder.setSecretKey(secretKey.toProtobuf());
    }
    return builder.build();
  }

  private SCMGetCurrentSecretKeyResponse getCurrentSecretKey()
      throws IOException {
    return SCMGetCurrentSecretKeyResponse.newBuilder()
        .setSecretKey(impl.getCurrentSecretKey().toProtobuf())
        .build();
  }

  private SCMGetCheckAndRotateResponse checkAndRotate(boolean force)
      throws IOException {
    return SCMGetCheckAndRotateResponse.newBuilder()
        .setStatus(impl.checkAndRotate(force)).build();
  }

  private Status exceptionToResponseStatus(IOException ex) {
    if (ex instanceof SCMSecretKeyException) {
      return Status.values()[
          ((SCMSecretKeyException) ex).getErrorCode().ordinal()];
    } else {
      return Status.INTERNAL_ERROR;
    }
  }

}
