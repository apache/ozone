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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.hadoop.ipc_.RpcNoSuchProtocolException;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetKeyInfoRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.ClientId;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests for {@link HadoopRpcOMFollowerReadFailoverProxyProvider}.
 */
public class TestHadoopRpcOMFollowerReadFailoverProxyProvider {
  private static final long SLOW_RESPONSE_SLEEP_TIME = TimeUnit.SECONDS.toMillis(2);
  private static final String OM_SERVICE_ID = "om-service-test1";
  private static final String NODE_ID_BASE_STR = "omNode-";

  private HadoopRpcOMFollowerReadFailoverProxyProvider proxyProvider;
  private OzoneManagerProtocolPB retryProxy;
  private String[] omNodeIds;
  private OMAnswer[] omNodeAnswers;

  @Test
  void testWriteOperationOnLeader() throws Exception {
    setupProxyProvider(3);
    omNodeAnswers[2].isLeader = true;

    doWrite();

    assertHandledBy(2);
    assertTrue(proxyProvider.isUseFollowerRead());
    // Although the write request is forwarded to the leader,
    // the follower read proxy provider should still point to first OM follower
    assertEquals(proxyProvider.getCurrentProxy().getNodeId(), omNodeIds[0]);
  }

  @Test
  void testWriteOperationOnLeaderNotReady() throws Exception {
    setupProxyProvider(3);
    omNodeAnswers[0].isLeader = true;
    omNodeAnswers[0].isLeaderReady = false;

    new Thread(() -> {
      try {
        Thread.sleep(1000);
        omNodeAnswers[0].isLeaderReady = true;
      } catch (InterruptedException ignored) {
      }
    }).start();

    long start = Time.monotonicNow();
    doWrite();
    long elapsed = Time.monotonicNow() - start;

    assertTrue(elapsed > 1000,
        "Write operation finished earlier than expected");

    assertHandledBy(0);
    assertTrue(proxyProvider.isUseFollowerRead());
  }

  @Test
  void testWriteLeaderFailover() throws Exception {
    setupProxyProvider(3);
    omNodeAnswers[0].isLeader = true;

    doWrite();
    assertHandledBy(0);
    // Read current proxy remain unchanged
    assertEquals(proxyProvider.getCurrentProxy().getNodeId(), omNodeIds[0]);


    // Leader failover from omNode-1 to omNode-2
    omNodeAnswers[0].isLeader = false;
    omNodeAnswers[1].isLeader = true;
    doWrite();
    assertHandledBy(1);
    assertEquals(proxyProvider.getCurrentProxy().getNodeId(), omNodeIds[0]);

    // Leader failover back from omNode-2 to omNode-1
    omNodeAnswers[0].isLeader = true;
    omNodeAnswers[1].isLeader = false;
    doWrite();
    assertHandledBy(0);
    assertEquals(proxyProvider.getCurrentProxy().getNodeId(), omNodeIds[0]);
  }

  @Test
  void testReadOperationOnFollower() throws Exception {
    setupProxyProvider(3);
    omNodeAnswers[0].isLeader = false;

    doRead();

    assertHandledBy(0);
    assertTrue(proxyProvider.isUseFollowerRead());
  }

  @Test
  void testReadOperationOnLeader() throws Exception {
    setupProxyProvider(3);
    omNodeAnswers[0].isLeader = true;

    doRead();

    // Follower read can still read from OM leader
    assertHandledBy(0);
    assertTrue(proxyProvider.isUseFollowerRead());
  }

  @Test
  void testReadOperationOnLeaderNotReady() throws Exception {
    setupProxyProvider(3);
    omNodeAnswers[0].isLeader = true;
    omNodeAnswers[0].isLeaderReady = false;

    new Thread(() -> {
      try {
        Thread.sleep(1000);
        omNodeAnswers[0].isLeaderReady = true;
      } catch (InterruptedException ignored) {
      }
    }).start();

    long start = Time.monotonicNow();
    doRead();
    long elapsed = Time.monotonicNow() - start;

    assertTrue(elapsed > 1000,
        "Read operation finished earlier than expected");

    assertHandledBy(0);
    assertTrue(proxyProvider.isUseFollowerRead());
  }

  @Test
  void testReadOperationOnFollowerWhenFollowerReadUnsupported() throws Exception {
    setupProxyProvider(3);
    // Disable all follower reads from all OM nodes
    for (OMAnswer omAnswer : omNodeAnswers) {
      omAnswer.isFollowerReadSupported = false;
    }
    omNodeAnswers[1].isLeader = true;

    doRead();
    // The read request will be handled by the leader
    assertHandledBy(1);
    // Since OMNotLeaderException is thrown during follower read, the
    // proxy will keep sending reads from the leader from now on
    assertFalse(proxyProvider.isUseFollowerRead());

    // Try to simulate leader change
    omNodeAnswers[1].isLeader = false;
    omNodeAnswers[2].isLeader = true;

    doRead();
    assertHandledBy(2);

    assertFalse(proxyProvider.isUseFollowerRead());
  }

  @Test
  void testUnreachableFollowers() throws Exception {
    setupProxyProvider(3);
    omNodeAnswers[2].isLeader = true;
    // Mark the first follower as unreachable
    omNodeAnswers[0].unreachable = true;

    // It will be handled by the second follower since the first follower is
    // unreachable
    doRead();
    assertHandledBy(1);

    // Now make the second follower as unavailable
    // All followers are unreachable now
    omNodeAnswers[1].unreachable = true;

    // Confirm that read still succeeds even though followers are not available
    doRead();
    // It will be handled by the leader
    assertHandledBy(2);
  }

  @Test
  void testReadOnSlowFollower() throws Exception {
    setupProxyProvider(3);
    omNodeAnswers[0].slowNode = true;

    long start = Time.monotonicNow();
    doRead();
    long elapsed = Time.monotonicNow() - start;
    assertHandledBy(0);
    assertThat(elapsed)
        .withFailMessage(() -> "Read operation finished earlier than expected")
        .isGreaterThanOrEqualTo(SLOW_RESPONSE_SLEEP_TIME);

  }

  @Test
  void testMixedWriteAndRead() throws Exception {
    setupProxyProvider(3);
    omNodeAnswers[1].isLeader = true;

    doWrite();

    // Write is handled by the leader
    assertHandledBy(1);

    doRead();

    // Read is handled by the first follower
    assertHandledBy(0);
  }

  @Test
  void testWriteWithAllOMsUnreachable() throws Exception {
    setupProxyProvider(3);
    omNodeAnswers[0].unreachable = true;
    omNodeAnswers[1].unreachable = true;
    omNodeAnswers[2].unreachable = true;

    ServiceException exception = assertThrows(ServiceException.class, this::doWrite);
    assertInstanceOf(IOException.class, exception.getCause());
  }

  @Test
  void testReadWithAllOMsUnreachable() throws Exception {
    setupProxyProvider(3);
    omNodeAnswers[0].unreachable = true;
    omNodeAnswers[1].unreachable = true;
    omNodeAnswers[2].unreachable = true;

    ServiceException exception = assertThrows(ServiceException.class, this::doRead);
    assertInstanceOf(IOException.class, exception.getCause());
  }

  @Test
  void testObjectMethodsOnProxy() throws Exception {
    setupProxyProvider(2);

    assertNotNull(retryProxy.toString());
    retryProxy.hashCode();
    retryProxy.equals(retryProxy);
  }

  @Test
  void testObjectMethodsDoNotSelectProxy() throws Exception {
    setupProxyProvider(2);

    assertNull(proxyProvider.getLastProxy());
  }

  @Test
  void testShortArgsArrayDoesNotThrowArrayIndex() throws Exception {
    setupProxyProvider(2);

    Object combinedProxy = proxyProvider.getProxy().proxy;
    InvocationHandler handler = Proxy.getInvocationHandler(combinedProxy);
    Method submitRequest = OzoneManagerProtocolPB.class.getMethod(
        "submitRequest", RpcController.class, OMRequest.class);

    ServiceException exception = assertThrows(ServiceException.class,
        () -> handler.invoke(combinedProxy, submitRequest, new Object[] {null}));
    assertInstanceOf(RpcNoSuchProtocolException.class, exception.getCause());
  }

  @Test
  void testNullRequest() throws Exception {
    setupProxyProvider(2);
    ServiceException exception = assertThrows(ServiceException.class,
        () -> retryProxy.submitRequest(null, null));
    assertInstanceOf(RpcNoSuchProtocolException.class, exception.getCause());
  }

  private void setupProxyProvider(int omNodeCount) throws Exception {
    setupProxyProvider(omNodeCount, new OzoneConfiguration());
  }

  private void setupProxyProvider(int omNodeCount, OzoneConfiguration config) throws Exception {
    omNodeIds = new String[omNodeCount];
    omNodeAnswers = new OMAnswer[omNodeCount];
    StringJoiner allNodeIds = new StringJoiner(",");
    final OzoneManagerProtocolPB[] proxies = new OzoneManagerProtocolPB[omNodeCount];
    for (int i = 0; i < omNodeCount; i++) {
      String nodeId = NODE_ID_BASE_STR + (i + 1); // 1-th indexed
      config.set(ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, OM_SERVICE_ID,
          nodeId),  "0.0.0.0:" + i);
      allNodeIds.add(nodeId);
      omNodeIds[i] = nodeId;
      omNodeAnswers[i] = new OMAnswer();
      proxies[i] = mock(OzoneManagerProtocolPB.class);
      doAnswer(omNodeAnswers[i].clientAnswer)
          .when(proxies[i]).submitRequest(any(), any());
      doAnswer(omNodeAnswers[i].clientAnswer)
          .when(proxies[i]).submitRequest(any(), any());
    }
    config.set(ConfUtils.addKeySuffixes(OZONE_OM_NODES_KEY, OM_SERVICE_ID),
        allNodeIds.toString());
    config.set(OZONE_OM_SERVICE_IDS_KEY, OM_SERVICE_ID);
    config.setInt(OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, 2 * omNodeCount);
    config.setLong(
        OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY, 500);

    // Create a leader-based failover proxy provider using the mocked proxies
    HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> underlyingProxyProvider =
        new HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB>(config, UserGroupInformation.getCurrentUser(),
            OM_SERVICE_ID, OzoneManagerProtocolPB.class) {
          @Override
          protected OzoneManagerProtocolPB createOMProxy(InetSocketAddress omAddress) {
            return proxies[omAddress.getPort()];
          }

          @Override
          protected List<OMProxyInfo<OzoneManagerProtocolPB>> initOmProxiesFromConfigs(
              ConfigurationSource config, String omSvcId) {
            List<OMProxyInfo<OzoneManagerProtocolPB>> omProxies = new ArrayList<>();

            Collection<String> activeOmNodeIds = OmUtils.getActiveOMNodeIds(config,
                omSvcId);

            for (String nodeId : OmUtils.emptyAsSingletonNull(activeOmNodeIds)) {

              String rpcAddrKey = ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
                  omSvcId, nodeId);
              String rpcAddrStr = OmUtils.getOmRpcAddress(config, rpcAddrKey);
              if (rpcAddrStr == null) {
                continue;
              }

              // ProxyInfo.proxy will be set during first time call to server.
              final OMProxyInfo<OzoneManagerProtocolPB> omProxyInfo = OMProxyInfo.newInstance(
                  null, omSvcId, nodeId, rpcAddrStr);

              if (omProxyInfo.getAddress() != null) {
                omProxies.add(omProxyInfo);
              } else {
                LOG.error("Failed to create OM proxy for {} at address {}",
                    nodeId, rpcAddrStr);
              }
            }

            if (omProxies.isEmpty()) {
              throw new IllegalArgumentException("Could not find any configured " +
                  "addresses for OM. Please configure the system with "
                  + OZONE_OM_ADDRESS_KEY);
            }
            // By default, the omNodesInOrder is shuffled to reduce hotspot, but we can sort it here to
            // make it easier to test
            omProxies.sort(Comparator.comparing(OMProxyInfo::getNodeId));
            return omProxies;
          }
        };

    // Wrap the leader-based failover proxy provider with follower read proxy provider
    proxyProvider = new HadoopRpcOMFollowerReadFailoverProxyProvider(underlyingProxyProvider);
    assertTrue(proxyProvider.isUseFollowerRead());
    // Wrap the follower read proxy provider in retry proxy to allow automatic failover
    retryProxy = (OzoneManagerProtocolPB) RetryProxy.create(
        OzoneManagerProtocolPB.class, proxyProvider,
        proxyProvider.getRetryPolicy(2 * omNodeCount)
    );
    // This is currently added to prevent IllegalStateException in
    // Client#setCallIdAndRetryCount since it seems that callId is set but not unset properly
    RetryInvocationHandler.SET_CALL_ID_FOR_TEST.set(false);
  }

  private void doRead() throws Exception {
    doRead(retryProxy);
  }

  private void doWrite() throws Exception {
    doWrite(retryProxy);
  }

  private static void doWrite(OzoneManagerProtocolPB client) throws Exception {
    CreateKeyRequest.Builder req = CreateKeyRequest.newBuilder();
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName("volume")
        .setBucketName("bucket")
        .setKeyName("key")
        .build();
    req.setKeyArgs(keyArgs);

    OMRequest omRequest = OMRequest.newBuilder()
        .setVersion(ClientVersion.CURRENT_VERSION)
        .setClientId(ClientId.randomId().toString())
        .setCmdType(Type.CreateKey)
        .setCreateKeyRequest(req)
        .build();

    client.submitRequest(null, omRequest);
  }

  private static void doRead(OzoneManagerProtocolPB client) throws Exception {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName("volume")
        .setBucketName("bucket")
        .setKeyName("key")
        .build();
    GetKeyInfoRequest.Builder req = GetKeyInfoRequest.newBuilder()
        .setKeyArgs(keyArgs);

    OMRequest omRequest = OMRequest.newBuilder()
        .setVersion(ClientVersion.CURRENT_VERSION)
        .setClientId(ClientId.randomId().toString())
        .setCmdType(Type.GetKeyInfo)
        .setGetKeyInfoRequest(req)
        .build();

    client.submitRequest(null, omRequest);
  }

  private void assertHandledBy(int omNodeIdx) {
    OMProxyInfo<OzoneManagerProtocolPB> lastProxy =
        (OMProxyInfo<OzoneManagerProtocolPB>) proxyProvider.getLastProxy();
    assertEquals(omNodeIds[omNodeIdx], lastProxy.getNodeId());
  }

  private static class OMAnswer {

    private volatile boolean unreachable = false;
    private volatile boolean slowNode = false;

    private volatile boolean isLeader = false;
    private volatile boolean isLeaderReady = true;
    private volatile boolean isFollowerReadSupported = true;

    private OMProtocolAnswer clientAnswer = new OMProtocolAnswer();

    private class OMProtocolAnswer implements Answer<OMResponse> {
      @Override
      public OMResponse answer(InvocationOnMock invocationOnMock) throws Throwable {
        if (unreachable) {
          throw new IOException("Unavailable");
        }

        // sleep to simulate slow rpc responses.
        if (slowNode) {
          Thread.sleep(SLOW_RESPONSE_SLEEP_TIME);
        }
        OMRequest omRequest = invocationOnMock.getArgument(1);
        switch (omRequest.getCmdType()) {
        case CreateKey:
          if (!isLeader) {
            throw new ServiceException(
                new RemoteException(
                    OMNotLeaderException.class.getCanonicalName(),
                    "Write can only be done on leader"
                )
            );
          }
          if (isLeader && !isLeaderReady) {
            throw new ServiceException(
                new RemoteException(
                    OMLeaderNotReadyException.class.getCanonicalName(),
                    "Leader is not ready yet"
                )
            );
          }
          break;
        case GetKeyInfo:
          if (!isLeader && !isFollowerReadSupported) {
            throw new ServiceException(
                new RemoteException(
                    OMNotLeaderException.class.getCanonicalName(),
                    "OM follower read is not supported"
                )
            );
          }
          if (isLeader && !isLeaderReady) {
            throw new ServiceException(
                new RemoteException(
                    OMLeaderNotReadyException.class.getCanonicalName(),
                    "Leader is not ready yet"
                )
            );
          }
          break;
        default:
          throw new IllegalArgumentException("Unsupported cmdType");
        }
        return null;
      }
    }
  }
}
