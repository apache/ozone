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
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.S3AuthenticationContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Captures request context before Ratis submission and installs it while an OM
 * request is executed by Ratis.
 *
 * <p>Request-scoped thread-local state that is needed during Ratis queries
 * must be serialized by {@link #captureIntoRequest(OMRequest, OzoneManager)}.
 * Write requests capture their context during {@link OMClientRequest#preExecute(OzoneManager)}.
 * Context used during either Ratis execution path must be installed by this class.
 * {@link #close()} restores context for reads, which Ratis may execute on the calling thread,
 * and clears context for writes executed by the StateMachineUpdater thread.
 */
public final class OMRatisRequestContext implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(OMRatisRequestContext.class);

  private enum Operation {
    READ,
    WRITE
  }

  private final Server.Call currentCall;
  private final Server.Call previousCall;
  private final S3AuthenticationContext previousS3Context;
  private final Operation operation;

  private OMRatisRequestContext(OMRequest request, OzoneManager ozoneManager, Operation operation)
      throws IOException {
    Objects.requireNonNull(ozoneManager, "ozoneManager");
    this.operation = Objects.requireNonNull(operation, "operation");
    boolean isRead = operation == Operation.READ;
    previousCall = isRead ? Server.getCurCall().get() : null;
    previousS3Context = isRead ? S3AuthenticationContext.capture() : null;
    currentCall = isRead ? createCall(request) : null;

    clear();
    try {
      if (currentCall != null) {
        Server.getCurCall().set(currentCall);
      }
      S3AuthenticationContext.fromRequest(request, ozoneManager.isSecurityEnabled()).install();
    } catch (IOException | RuntimeException ex) {
      close();
      throw ex;
    }
  }

  /**
   * Captures authenticated request context on the RPC thread before submitting
   * a read request to Ratis.
   */
  public static OMRequest captureIntoRequest(OMRequest request, OzoneManager ozoneManager) throws IOException {
    Objects.requireNonNull(ozoneManager, "ozoneManager");
    OMRequest.Builder requestBuilder = request.toBuilder()
        .setUserInfo(OMClientRequest.getAuthenticatedUserInfo(request));
    S3AuthenticationContext.captureInto(requestBuilder);
    return requestBuilder.build();
  }

  /**
   * Installs context for a Ratis read, including a synthetic Hadoop RPC call
   * used by existing read implementations and lock accounting. Any context
   * already present on the calling thread is restored when this scope closes.
   */
  public static OMRatisRequestContext openForRead(OMRequest request, OzoneManager ozoneManager)
      throws IOException {
    return new OMRatisRequestContext(request, ozoneManager, Operation.READ);
  }

  /**
   * Installs context for a Ratis write without a Hadoop RPC call, preserving
   * write lock accounting through ResourceLockTracker.
   */
  public static OMRatisRequestContext openForWrite(OMRequest request, OzoneManager ozoneManager)
      throws IOException {
    return new OMRatisRequestContext(request, ozoneManager, Operation.WRITE);
  }

  /**
   * Adds query lock timings from the synthetic Hadoop RPC call to the response.
   */
  public OMResponse addLockDetails(OMResponse response) {
    return currentCall == null ? response
        : OMLockDetailsUtil.addToResponse(response, currentCall.getProcessingDetails());
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

  private static void clear() {
    Server.getCurCall().remove();
    S3AuthenticationContext.clear();
  }

  @Override
  public void close() {
    if (operation == Operation.READ) {
      if (previousCall == null) {
        Server.getCurCall().remove();
      } else {
        Server.getCurCall().set(previousCall);
      }
      previousS3Context.install();
    } else {
      clear();
    }
  }
}
