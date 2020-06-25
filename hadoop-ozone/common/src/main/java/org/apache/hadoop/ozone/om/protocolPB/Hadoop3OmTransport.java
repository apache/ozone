/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.protocolPB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Full-featured Hadoop RPC implementation with failover support.
 */
public class Hadoop3OmTransport implements OmTransport {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private static final Logger LOG =
      LoggerFactory.getLogger(Hadoop3OmTransport.class);

  private final OMFailoverProxyProvider omFailoverProxyProvider;

  private final OzoneManagerProtocolPB rpcProxy;
  private List<String> retryExceptions = new ArrayList<>();

  public Hadoop3OmTransport(ConfigurationSource conf,
      UserGroupInformation ugi, String omServiceId) throws IOException {

    RPC.setProtocolEngine(OzoneConfiguration.of(conf),
        OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);

    this.omFailoverProxyProvider = new OMFailoverProxyProvider(conf, ugi,
        omServiceId);

    int maxFailovers = conf.getInt(
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT);

    this.rpcProxy = createRetryProxy(omFailoverProxyProvider, maxFailovers);
  }

  @Override
  public OMResponse submitRequest(OMRequest payload) throws IOException {
    try {
      OMResponse omResponse =
          rpcProxy.submitRequest(NULL_RPC_CONTROLLER, payload);

      if (omResponse.hasLeaderOMNodeId() && omFailoverProxyProvider != null) {
        String leaderOmId = omResponse.getLeaderOMNodeId();

        // Failover to the OM node returned by OMResponse leaderOMNodeId if
        // current proxy is not pointing to that node.
        omFailoverProxyProvider.performFailoverIfRequired(leaderOmId);
      }
      return omResponse;
    } catch (ServiceException e) {
      OMNotLeaderException notLeaderException = getNotLeaderException(e);
      if (notLeaderException == null) {
        throw ProtobufHelper.getRemoteException(e);
      }
      throw new IOException("Could not determine or connect to OM Leader.");
    }
  }

  @Override
  public Text getDelegationTokenService() {
    return omFailoverProxyProvider.getCurrentProxyDelegationToken();
  }

  /**
   * Creates a {@link RetryProxy} encapsulating the
   * {@link OMFailoverProxyProvider}. The retry proxy fails over on network
   * exception or if the current proxy is not the leader OM.
   */
  private OzoneManagerProtocolPB createRetryProxy(
      OMFailoverProxyProvider failoverProxyProvider, int maxFailovers) {

    // Client attempts contacting each OM ipc.client.connect.max.retries
    // (default = 10) times before failing over to the next OM, if
    // available.
    // Client will attempt upto maxFailovers number of failovers between
    // available OMs before throwing exception.
    RetryPolicy retryPolicy = new RetryPolicy() {
      @Override
      public RetryAction shouldRetry(Exception exception, int retries,
          int failovers, boolean isIdempotentOrAtMostOnce)
          throws Exception {
        if (isAccessControlException(exception)) {
          return RetryAction.FAIL; // do not retry
        }
        if (exception instanceof ServiceException) {
          OMNotLeaderException notLeaderException =
              getNotLeaderException(exception);
          if (notLeaderException != null) {
            retryExceptions.add(getExceptionMsg(notLeaderException, failovers));
            if (LOG.isDebugEnabled()) {
              LOG.debug("RetryProxy: {}", notLeaderException.getMessage());
            }

            // TODO: NotLeaderException should include the host
            //  address of the suggested leader along with the nodeID.
            //  Failing over just based on nodeID is not very robust.

            // OMFailoverProxyProvider#performFailover() is a dummy call and
            // does not perform any failover. Failover manually to the next OM.
            omFailoverProxyProvider.performFailoverToNextProxy();
            return getRetryAction(RetryDecision.FAILOVER_AND_RETRY, failovers);
          }

          OMLeaderNotReadyException leaderNotReadyException =
              getLeaderNotReadyException(exception);
          // As in this case, current OM node is leader, but it is not ready.
          // OMFailoverProxyProvider#performFailover() is a dummy call and
          // does not perform any failover.
          // So Just retry with same OM node.
          if (leaderNotReadyException != null) {
            retryExceptions.add(getExceptionMsg(leaderNotReadyException,
                failovers));
            if (LOG.isDebugEnabled()) {
              LOG.debug("RetryProxy: {}", leaderNotReadyException.getMessage());
            }
            // HDDS-3465. OM index will not change, but LastOmID will be
            // updated to currentOMId, so that waitTime calculation will
            // know lastOmID and currentID are same and need to increment
            // wait time in between.
            omFailoverProxyProvider.performFailoverIfRequired(
                omFailoverProxyProvider.getCurrentProxyOMNodeId());
            return getRetryAction(RetryDecision.FAILOVER_AND_RETRY, failovers);
          }
        }

        // For all other exceptions other than LeaderNotReadyException and
        // NotLeaderException fail over manually to the next OM Node proxy.
        // OMFailoverProxyProvider#performFailover() is a dummy call and
        // does not perform any failover.
        retryExceptions.add(getExceptionMsg(exception, failovers));
        if (LOG.isDebugEnabled()) {
          LOG.debug("RetryProxy: {}", exception.getCause() != null ?
              exception.getCause().getMessage() : exception.getMessage());
        }
        omFailoverProxyProvider.performFailoverToNextProxy();
        return getRetryAction(RetryDecision.FAILOVER_AND_RETRY, failovers);
      }

      private RetryAction getRetryAction(RetryDecision fallbackAction,
          int failovers) {
        if (failovers < maxFailovers) {
          return new RetryAction(fallbackAction,
              omFailoverProxyProvider.getWaitTime());
        } else {
          StringBuilder allRetryExceptions = new StringBuilder();
          allRetryExceptions.append("\n");
          retryExceptions.stream().forEach(e -> allRetryExceptions.append(e));
          LOG.error("Failed to connect to OMs: {}. Attempted {} failovers. " +
                  "Got following exceptions during retries: {}",
              omFailoverProxyProvider.getOMProxyInfos(), maxFailovers,
              allRetryExceptions.toString());
          retryExceptions.clear();
          return RetryAction.FAIL;
        }
      }
    };

    OzoneManagerProtocolPB proxy = (OzoneManagerProtocolPB) RetryProxy.create(
        OzoneManagerProtocolPB.class, failoverProxyProvider, retryPolicy);
    return proxy;
  }

  private String getExceptionMsg(Exception e, int retryAttempt) {
    StringBuilder exceptionMsg = new StringBuilder()
        .append("Retry Attempt ")
        .append(retryAttempt)
        .append(" Exception - ");
    if (e.getCause() == null) {
      exceptionMsg.append(e.getClass().getCanonicalName())
          .append(": ")
          .append(e.getMessage());
    } else {
      exceptionMsg.append(e.getCause().getClass().getCanonicalName())
          .append(": ")
          .append(e.getCause().getMessage());
    }
    return exceptionMsg.toString();
  }

  /**
   * Check if exception is OMLeaderNotReadyException.
   *
   * @param exception
   * @return OMLeaderNotReadyException
   */
  private OMLeaderNotReadyException getLeaderNotReadyException(
      Exception exception) {
    Throwable cause = exception.getCause();
    if (cause instanceof RemoteException) {
      IOException ioException =
          ((RemoteException) cause).unwrapRemoteException();
      if (ioException instanceof OMLeaderNotReadyException) {
        return (OMLeaderNotReadyException) ioException;
      }
    }
    return null;
  }

  /**
   * Unwrap exception to check if it is some kind of access control problem
   * ({@link AccessControlException} or {@link SecretManager.InvalidToken}).
   */
  private boolean isAccessControlException(Exception ex) {
    if (ex instanceof ServiceException) {
      Throwable t = ex.getCause();
      if (t instanceof RemoteException) {
        t = ((RemoteException) t).unwrapRemoteException();
      }
      while (t != null) {
        if (t instanceof AccessControlException ||
            t instanceof SecretManager.InvalidToken) {
          return true;
        }
        t = t.getCause();
      }
    }
    return false;
  }

  /**
   * Check if exception is a OMNotLeaderException.
   *
   * @return OMNotLeaderException.
   */
  private OMNotLeaderException getNotLeaderException(Exception exception) {
    Throwable cause = exception.getCause();
    if (cause instanceof RemoteException) {
      IOException ioException =
          ((RemoteException) cause).unwrapRemoteException();
      if (ioException instanceof OMNotLeaderException) {
        return (OMNotLeaderException) ioException;
      }
    }
    return null;
  }

  @VisibleForTesting
  public OMFailoverProxyProvider getOmFailoverProxyProvider() {
    return omFailoverProxyProvider;
  }

  @Override
  public void close() throws IOException {
    omFailoverProxyProvider.close();
  }
}
