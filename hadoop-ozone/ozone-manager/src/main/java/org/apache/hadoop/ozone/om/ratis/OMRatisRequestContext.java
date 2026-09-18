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

package org.apache.hadoop.ozone.om.ratis;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.ipc_.ProcessingDetails.Timing;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.RpcConstants;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.STSSecurityUtil;
import org.apache.hadoop.ozone.security.STSTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Captures request context before Ratis submission and installs it while an OM
 * request is executed by Ratis.
 *
 * <p>Request-scoped thread-local state that is needed during Ratis queries
 * must be serialized by {@link #capture(OMRequest, OzoneManager)}. Write
 * requests capture their context during
 * {@link OMClientRequest#preExecute(OzoneManager)}. Context used during either
 * Ratis execution path must be installed by this class and cleared by
 * {@link #close()}.
 */
public final class OMRatisRequestContext implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(OMRatisRequestContext.class);

  private final Server.Call currentCall;

  private OMRatisRequestContext(OMRequest request, OzoneManager ozoneManager, boolean installRpcCall)
      throws IOException {
    Objects.requireNonNull(ozoneManager, "ozoneManager");
    currentCall = installRpcCall ? createCall(request) : null;

    clear();
    try {
      if (currentCall != null) {
        Server.getCurCall().set(currentCall);
      }
      if (ozoneManager.isSecurityEnabled() && request.hasS3Authentication()) {
        STSSecurityUtil.ensureResolvedStsFieldsInvariants(request);
        OzoneManagerProtocolProtos.S3Authentication s3Auth = request.getS3Authentication();
        OzoneManager.setS3Auth(s3Auth);
        if (s3Auth.hasSessionToken() && !s3Auth.getSessionToken().isEmpty()) {
          OzoneManager.setStsTokenIdentifier(rehydrateStsTokenIdentifier(s3Auth));
        }
      }
    } catch (IOException | RuntimeException ex) {
      close();
      throw ex;
    }
  }

  /**
   * Captures authenticated request context on the RPC thread before submitting
   * a read request to Ratis.
   */
  public static OMRequest capture(OMRequest request, OzoneManager ozoneManager) throws IOException {
    Objects.requireNonNull(ozoneManager, "ozoneManager");
    OMRequest.Builder requestBuilder = request.toBuilder()
        .setUserInfo(OMClientRequest.getAuthenticatedUserInfo(request));
    if (request.hasS3Authentication()) {
      requestBuilder.setS3Authentication(OMClientRequest.resolveS3Authentication(
          request.getS3Authentication(), OzoneManager.getStsTokenIdentifier()));
    }
    return requestBuilder.build();
  }

  /**
   * Installs context for a Ratis query, including a synthetic Hadoop RPC call
   * used by existing read implementations and lock accounting.
   */
  public static OMRatisRequestContext openForQuery(OMRequest request, OzoneManager ozoneManager)
      throws IOException {
    return new OMRatisRequestContext(request, ozoneManager, true);
  }

  /**
   * Installs context for a Ratis write without a Hadoop RPC call, preserving
   * write lock accounting through ResourceLockTracker.
   */
  public static OMRatisRequestContext openForWrite(OMRequest request, OzoneManager ozoneManager)
      throws IOException {
    return new OMRatisRequestContext(request, ozoneManager, false);
  }

  /**
   * Adds query lock timings from the synthetic Hadoop RPC call to the response.
   */
  public OMResponse addLockDetails(OMResponse response) {
    if (currentCall == null) {
      return response;
    }

    long waitNanos = currentCall.getProcessingDetails().get(Timing.LOCKWAIT, TimeUnit.NANOSECONDS);
    long readNanos = currentCall.getProcessingDetails().get(Timing.LOCKSHARED, TimeUnit.NANOSECONDS);
    long writeNanos = currentCall.getProcessingDetails().get(Timing.LOCKEXCLUSIVE, TimeUnit.NANOSECONDS);
    if (waitNanos == 0 && readNanos == 0 && writeNanos == 0) {
      return response;
    }

    OzoneManagerProtocolProtos.OMLockDetailsProto.Builder lockDetails = response.hasOmLockDetails()
        ? response.getOmLockDetails().toBuilder() : OzoneManagerProtocolProtos.OMLockDetailsProto.newBuilder();
    lockDetails.setWaitLockNanos(lockDetails.getWaitLockNanos() + waitNanos);
    lockDetails.setReadLockNanos(lockDetails.getReadLockNanos() + readNanos);
    lockDetails.setWriteLockNanos(lockDetails.getWriteLockNanos() + writeNanos);
    return response.toBuilder().setOmLockDetails(lockDetails).build();
  }

  private static Server.Call createCall(OMRequest request) {
    if (!request.hasUserInfo()) {
      return null;
    }

    OzoneManagerProtocolProtos.UserInfo userInfo = request.getUserInfo();
    UserGroupInformation remoteUser = userInfo.hasUserName() && !userInfo.getUserName().isEmpty()
        ? UserGroupInformation.createRemoteUser(userInfo.getUserName()) : null;
    InetAddress remoteAddress = createRemoteAddress(userInfo);
    if (remoteUser == null && remoteAddress == null) {
      return null;
    }

    return new Server.Call(RpcConstants.INVALID_CALL_ID, RpcConstants.INVALID_RETRY_COUNT,
        null, null, RPC.RpcKind.RPC_PROTOCOL_BUFFER, RpcConstants.DUMMY_CLIENT_ID) {
      @Override
      public UserGroupInformation getRemoteUser() {
        return remoteUser;
      }

      @Override
      public InetAddress getHostInetAddress() {
        return remoteAddress;
      }
    };
  }

  private static InetAddress createRemoteAddress(OzoneManagerProtocolProtos.UserInfo userInfo) {
    if (!userInfo.hasRemoteAddress() || userInfo.getRemoteAddress().isEmpty()) {
      return null;
    }

    try {
      InetAddress address = InetAddress.getByName(userInfo.getRemoteAddress());
      return userInfo.hasHostName() && !userInfo.getHostName().isEmpty()
          ? InetAddress.getByAddress(userInfo.getHostName(), address.getAddress()) : address;
    } catch (UnknownHostException ex) {
      LOG.debug("Unable to restore remote address from Ratis request context: {}",
          userInfo.getRemoteAddress(), ex);
      return null;
    }
  }

  private static STSTokenIdentifier rehydrateStsTokenIdentifier(
      OzoneManagerProtocolProtos.S3Authentication s3Auth) {
    // Context reaching Ratis has already passed expiry and revocation checks.
    return new STSTokenIdentifier(STSTokenIdentifier.Params.newBuilder()
        .setTempAccessKeyId(s3Auth.hasResolvedStsTempAccessKeyId() ? s3Auth.getResolvedStsTempAccessKeyId() : "")
        .setOriginalAccessKeyId(
            s3Auth.hasResolvedStsOriginalAccessKeyId() ? s3Auth.getResolvedStsOriginalAccessKeyId() : "")
        .setRoleArn(s3Auth.hasResolvedStsRoleArn() ? s3Auth.getResolvedStsRoleArn() : "")
        .setCreationTime(Instant.MAX)
        .setExpiry(Instant.MAX)
        .setSecretAccessKey(null)
        .setSessionPolicy(s3Auth.hasResolvedStsSessionPolicy() ? s3Auth.getResolvedStsSessionPolicy() : "")
        .setManagedSecretKey(null)
        .build());
  }

  private static void clear() {
    Server.getCurCall().remove();
    OzoneManager.setS3Auth(null);
    OzoneManager.setStsTokenIdentifier(null);
  }

  @Override
  public void close() {
    clear();
  }
}
