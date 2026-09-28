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
import java.util.Objects;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.RpcConstants;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.lock.OMLockDetailsUtil;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserInfo;
import org.apache.hadoop.ozone.security.STSSecurityUtil;
import org.apache.hadoop.ozone.security.STSTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Request-derived context that can be applied while OM executes a Ratis request.
 *
 * <p>Ratis may execute a read or write on the submitting thread or on a separate thread. Applying this context replaces
 * request-scoped state on whichever thread performs the operation, and closing the returned {@link Scope} restores the
 * state that was previously present on that thread.</p>
 */
public final class OMThreadContext {
  private static final Logger LOG = LoggerFactory.getLogger(OMThreadContext.class);

  private final UserInfo userInfo;
  private final S3Authentication s3Authentication;
  private final STSTokenIdentifier stsTokenIdentifier;

  private OMThreadContext(UserInfo userInfo, S3Authentication s3Authentication,
      STSTokenIdentifier stsTokenIdentifier) {
    this.userInfo = userInfo;
    this.s3Authentication = s3Authentication;
    this.stsTokenIdentifier = stsTokenIdentifier;
  }

  /**
   * Creates context for a Ratis read. Applying it creates a fresh synthetic Hadoop RPC call for the request.
   */
  public static OMThreadContext forRead(OMRequest request, OzoneManager ozoneManager) throws IOException {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(ozoneManager, "ozoneManager");
    UserInfo requestUserInfo = request.hasUserInfo() ? request.getUserInfo() : null;
    return fromRequest(requestUserInfo, request, ozoneManager);
  }

  /**
   * Creates context for a Ratis write. Writes do not install a synthetic Hadoop RPC call, preserving lock accounting
   * through ResourceLockTracker.
   */
  public static OMThreadContext forWrite(OMRequest request, OzoneManager ozoneManager) throws IOException {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(ozoneManager, "ozoneManager");
    return fromRequest(null, request, ozoneManager);
  }

  private static OMThreadContext fromRequest(UserInfo userInfo, OMRequest request,
      OzoneManager ozoneManager) throws IOException {
    if (!ozoneManager.isSecurityEnabled() || !request.hasS3Authentication()) {
      return new OMThreadContext(userInfo, null, null);
    }

    STSSecurityUtil.ensureResolvedStsFieldsInvariants(request);
    S3Authentication s3Authentication = request.getS3Authentication();
    return new OMThreadContext(userInfo, s3Authentication,
        STSSecurityUtil.rehydrateStsTokenIdentifier(s3Authentication));
  }

  /**
   * Replaces request-scoped state on the current thread until the returned scope is closed.
   */
  public Scope applyToCurrentThread() {
    Server.Call currentCall = createCall(userInfo);
    Scope scope = new Scope(Server.getCurCall().get(), OzoneManager.getS3Auth(),
        OzoneManager.getStsTokenIdentifier(), currentCall);
    install(currentCall, s3Authentication, stsTokenIdentifier);
    return scope;
  }

  private static Server.Call createCall(UserInfo userInfo) {
    if (userInfo == null) {
      return null;
    }

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

  private static InetAddress createRemoteAddress(UserInfo userInfo) {
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

  private static void install(Server.Call call, S3Authentication s3Authentication,
      STSTokenIdentifier stsTokenIdentifier) {
    if (call == null) {
      Server.getCurCall().remove();
    } else {
      Server.getCurCall().set(call);
    }
    OzoneManager.setS3Auth(s3Authentication);
    OzoneManager.setStsTokenIdentifier(stsTokenIdentifier);
  }

  /**
   * Applied OM request context. The scope must be closed on the thread where it was created.
   */
  public static final class Scope implements AutoCloseable {
    private final Thread ownerThread;
    private final Server.Call previousCall;
    private final S3Authentication previousS3Authentication;
    private final STSTokenIdentifier previousStsTokenIdentifier;
    private final Server.Call currentCall;
    private boolean closed;

    private Scope(Server.Call previousCall, S3Authentication previousS3Authentication,
        STSTokenIdentifier previousStsTokenIdentifier, Server.Call currentCall) {
      this.ownerThread = Thread.currentThread();
      this.previousCall = previousCall;
      this.previousS3Authentication = previousS3Authentication;
      this.previousStsTokenIdentifier = previousStsTokenIdentifier;
      this.currentCall = currentCall;
    }

    /**
     * Adds read lock timings from the synthetic Hadoop RPC call to the response.
     */
    public OMResponse addLockDetails(OMResponse response) {
      return currentCall == null ? response
          : OMLockDetailsUtil.addToResponse(response, currentCall.getProcessingDetails());
    }

    @Override
    public void close() {
      if (Thread.currentThread() != ownerThread) {
        throw new IllegalStateException(
            "OM thread context must be closed on the thread where it was applied");
      }
      if (closed) {
        return;
      }

      install(previousCall, previousS3Authentication, previousStsTokenIdentifier);
      closed = true;
    }
  }
}
