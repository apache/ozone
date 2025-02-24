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

package org.apache.hadoop.ozone.om.ha;

import static org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase.getLeaderNotReadyException;
import static org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase.getNotLeaderException;

import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link FailoverProxyProvider} which does nothing in the
 * event of OM failover, and always returns the same proxy object. In case of OM failover,
 * client will keep retrying to connect to the same OM node.
 */
public abstract class SingleOMFailoverProxyProviderBase<T> implements FailoverProxyProvider<T> {

  public static final Logger LOG =
      LoggerFactory.getLogger(SingleOMFailoverProxyProviderBase.class);

  private final ConfigurationSource conf;
  private final Class<T> protocolClass;

  private final String omServiceId;
  private final String omNodeId;
  private ProxyInfo<T> omProxy;

  private final long waitBetweenRetries;

  private final UserGroupInformation ugi;

  public SingleOMFailoverProxyProviderBase(ConfigurationSource configuration, UserGroupInformation ugi,
                                           String omServiceId, String omNodeId, Class<T> protocol) throws IOException {
    this.conf = configuration;
    this.protocolClass = protocol;
    this.omServiceId = omServiceId;
    this.omNodeId = omNodeId;
    this.ugi = ugi;

    waitBetweenRetries = conf.getLong(
        OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY,
        OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_DEFAULT);

    loadOMClientConfig(conf, omServiceId, omNodeId);
    Preconditions.checkNotNull(omProxy);
  }

  protected abstract void loadOMClientConfig(ConfigurationSource config, String omSvcId,
                                             String omNodeID) throws IOException;

  /**
   * Get the protocol proxy for provided address.
   * @param omAddress An instance of {@link InetSocketAddress} which contains the address to connect
   * @return the proxy connection to the address and the set of methods supported by the server at the address
   * @throws IOException if any error occurs while trying to get the proxy
   */
  protected T createOMProxy(InetSocketAddress omAddress) throws IOException {
    Configuration hadoopConf =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(getConf());

    // TODO: Post upgrade to Protobuf 3.x we need to use ProtobufRpcEngine2
    RPC.setProtocolEngine(hadoopConf, getInterface(), ProtobufRpcEngine.class);

    // Ensure we fail immediately in case of OM exceptions
    RetryPolicy connectionRetryPolicy = RetryPolicies.failoverOnNetworkException(0);

    return (T) RPC.getProtocolProxy(
        getInterface(),
        RPC.getProtocolVersion(protocolClass),
        omAddress,
        ugi,
        hadoopConf,
        NetUtils.getDefaultSocketFactory(hadoopConf),
        (int) OmUtils.getOMClientRpcTimeOut(getConf()),
        connectionRetryPolicy
    ).getProxy();
  }

  @Override
  public ProxyInfo<T> getProxy() {
    return omProxy;
  }

  @Override
  public final Class<T> getInterface() {
    return protocolClass;
  }

  protected synchronized boolean shouldRetryAgain(Exception ex) {
    Throwable unwrappedException = HddsUtils.getUnwrappedException(ex);
    if (unwrappedException instanceof AccessControlException ||
        unwrappedException instanceof SecretManager.InvalidToken ||
        HddsUtils.shouldNotFailoverOnRpcException(unwrappedException)) {
      // Unlike OMFailoverProxyProviderBase which will retry all OMs first,
      // AccessControlException will not be retried for single-OM case.
      return false;
    } else if (ex instanceof StateMachineException) {
      StateMachineException smEx = (StateMachineException) ex;
      Throwable cause = smEx.getCause();
      if (cause instanceof OMException) {
        OMException omEx = (OMException) cause;
        // Do not retry if the operation was blocked because the OM was
        // prepared.
        return omEx.getResult() !=
            OMException.ResultCodes.NOT_SUPPORTED_OPERATION_WHEN_PREPARED;
      }
    }
    return true;
  }

  public synchronized long getWaitTime() {
    return waitBetweenRetries;
  }

  public RetryPolicy getRetryPolicy(int maxRetries) {
    RetryPolicy retryPolicy = new RetryPolicy() {
      @Override
      public RetryAction shouldRetry(Exception exception, int retries,
          int failovers, boolean isIdempotentOrAtMostOnce)
          throws Exception {

        if (LOG.isDebugEnabled()) {
          if (exception.getCause() != null) {
            LOG.debug("RetryProxy: OM {}: {}: {}", omNodeId,
                exception.getCause().getClass().getSimpleName(),
                exception.getCause().getMessage());
          } else {
            LOG.debug("RetryProxy: OM {}: {}", omNodeId,
                exception.getMessage());
          }
        }

        if (exception instanceof ServiceException) {
          OMNotLeaderException notLeaderException =
              getNotLeaderException(exception);
          if (notLeaderException != null) {
            // This means that the OM node is a follower and it does not allow this particular
            // request to go through. We should fail this immediately since the OM does not
            // allow the request to non-leader to go through.
            return RetryAction.FAIL;
          }

          OMLeaderNotReadyException leaderNotReadyException =
              getLeaderNotReadyException(exception);
          if (leaderNotReadyException != null) {
            // Retry on same OM again as leader OM is not ready.
            // Failing over to same OM so that wait time between retries is
            // incremented
            return getRetryAction(RetryDecision.RETRY, retries);
          }
        }

        if (!shouldRetryAgain(exception)) {
          return RetryAction.FAIL; // do not retry
        }
        return getRetryAction(RetryDecision.RETRY, retries);
      }

      private RetryAction getRetryAction(RetryDecision fallbackAction, int retries) {
        if (retries < maxRetries) {
          return new RetryAction(fallbackAction, getWaitTime());
        } else {
          LOG.error("Failed to connect to OM node: {}. Attempted {} retries.",
              omNodeId, retries);
          return RetryAction.FAIL;
        }
      }
    };

    return retryPolicy;
  }

  @Override
  public final synchronized void performFailover(T currentProxy) {
    // Do nothing since this proxy provider does not failover to other OM
  }

  protected ConfigurationSource getConf() {
    return conf;
  }

  protected synchronized void setOmProxy(ProxyInfo<T> omProxy) {
    this.omProxy = omProxy;
  }

}
