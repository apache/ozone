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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.AccessControlException;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

/**
 * Tests SingleOMFailoverProxyProvider behaviour.
 */
public class TestSingleOMFailoverProxyProvider {
  private static final String OM_SERVICE_ID = "om-service-test1";
  private static final String NODE_ID_BASE_STR = "omNode-";
  private static final String DUMMY_NODE_ADDR = "0.0.0.0:8080";
  private static final Map<String, MockSingleFailoverProxyProvider>
      MOCK_PROVIDERS = new HashMap<>();
  // Configure the retries to speed up the tests
  private static final int MAX_ATTEMPTS = 5;
  private static final long WAIT_BETWEEN_RETRIES = 100L;
  private static final int NUM_OF_NODES = 3;
  private Exception testException;

  @BeforeEach
  void init() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    config.setInt(OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, MAX_ATTEMPTS);
    config.setLong(OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY, WAIT_BETWEEN_RETRIES);

    StringJoiner allNodeIds = new StringJoiner(",");
    for (int i = 1; i <= NUM_OF_NODES; i++) {
      String nodeId = NODE_ID_BASE_STR + i;
      config.set(ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, OM_SERVICE_ID,
          nodeId), DUMMY_NODE_ADDR);
      allNodeIds.add(nodeId);
      MOCK_PROVIDERS.put(nodeId, new MockSingleFailoverProxyProvider(config, nodeId));
    }
    config.set(ConfUtils.addKeySuffixes(OZONE_OM_NODES_KEY, OM_SERVICE_ID),
        allNodeIds.toString());
  }

  @Test
  public void testAccessControlExceptionNoFailover() throws IOException {
    testException = new AccessControlException();

    GenericTestUtils.setLogLevel(SingleOMFailoverProxyProviderBase.LOG, Level.DEBUG);
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(SingleOMFailoverProxyProviderBase.LOG);

    MockSingleFailoverProxyProvider singleFailoverProxyProvider = MOCK_PROVIDERS.get(NODE_ID_BASE_STR + "1");

    OzoneManagerProtocolPB proxy = (OzoneManagerProtocolPB) RetryProxy
        .create(OzoneManagerProtocolPB.class, singleFailoverProxyProvider,
            singleFailoverProxyProvider.getRetryPolicy(MAX_ATTEMPTS));

    ServiceException serviceException = assertThrows(ServiceException.class,
        () -> proxy.submitRequest(null, null));

    // Fail immediately for AccessControlException
    assertThat(serviceException).hasCauseInstanceOf(AccessControlException.class);
    assertThat(logCapturer.getOutput()).contains(
        getRetryProxyDebugMsg(NODE_ID_BASE_STR + "1"));
    assertThat(logCapturer.getOutput()).doesNotContain(
        getRetryProxyDebugMsg(NODE_ID_BASE_STR + "2"));
    assertThat(logCapturer.getOutput()).doesNotContain(
        getRetryProxyDebugMsg(NODE_ID_BASE_STR + "3"));
  }

  @Test
  public void testNotLeaderExceptionNoFailover() {
    OMNotLeaderException omNotLeaderException = new OMNotLeaderException(RaftPeerId.valueOf(NODE_ID_BASE_STR + "1"),
        RaftPeerId.valueOf(NODE_ID_BASE_STR + "2"), null);
    testException = new RemoteException(omNotLeaderException.getClass().getName(), omNotLeaderException.getMessage());


    GenericTestUtils.setLogLevel(SingleOMFailoverProxyProviderBase.LOG, Level.DEBUG);
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(SingleOMFailoverProxyProviderBase.LOG);

    MockSingleFailoverProxyProvider singleFailoverProxyProvider = MOCK_PROVIDERS.get(NODE_ID_BASE_STR + "1");

    OzoneManagerProtocolPB proxy = (OzoneManagerProtocolPB) RetryProxy
        .create(OzoneManagerProtocolPB.class, singleFailoverProxyProvider,
            singleFailoverProxyProvider.getRetryPolicy(MAX_ATTEMPTS));

    ServiceException serviceException = assertThrows(ServiceException.class,
        () -> proxy.submitRequest(null, null));

    // Fail immediately for OMNotLeaderException
    assertThat(serviceException).hasCauseInstanceOf(RemoteException.class);
    assertThat(logCapturer.getOutput()).contains(getRetryProxyDebugMsg(NODE_ID_BASE_STR + "1"));
    assertThat(logCapturer.getOutput()).doesNotContain(getRetryProxyDebugMsg(NODE_ID_BASE_STR + "2"));
    assertThat(logCapturer.getOutput()).doesNotContain(getRetryProxyDebugMsg(NODE_ID_BASE_STR + "3"));
  }

  @Test
  public void testLeaderNotReadyException() {
    OMLeaderNotReadyException omLeaderNotReadyException = new OMLeaderNotReadyException(
        NODE_ID_BASE_STR + "1 is Leader but not ready to process request yet.");
    testException = new RemoteException(omLeaderNotReadyException.getClass().getName(),
        omLeaderNotReadyException.getMessage());

    GenericTestUtils.setLogLevel(SingleOMFailoverProxyProviderBase.LOG, Level.DEBUG);
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(SingleOMFailoverProxyProviderBase.LOG);

    MockSingleFailoverProxyProvider singleFailoverProxyProvider = MOCK_PROVIDERS.get(NODE_ID_BASE_STR + "1");

    OzoneManagerProtocolPB proxy = (OzoneManagerProtocolPB) RetryProxy
        .create(OzoneManagerProtocolPB.class, singleFailoverProxyProvider,
            singleFailoverProxyProvider.getRetryPolicy(MAX_ATTEMPTS));

    ServiceException serviceException = assertThrows(ServiceException.class,
        () -> proxy.submitRequest(null, null));


    assertThat(serviceException).hasCauseInstanceOf(RemoteException.class);
    // Will retry up until max attempts
    for (int i = 0; i < MAX_ATTEMPTS; i++) {
      assertThat(logCapturer.getOutput()).contains(getRetryProxyDebugMsg(NODE_ID_BASE_STR + "1"));
    }
    assertThat(logCapturer.getOutput()).doesNotContain(getRetryProxyDebugMsg(NODE_ID_BASE_STR + "2"));
    assertThat(logCapturer.getOutput()).doesNotContain(getRetryProxyDebugMsg(NODE_ID_BASE_STR + "3"));
  }

  private String getRetryProxyDebugMsg(String omNodeId) {
    return "RetryProxy: OM " + omNodeId + ": " +
        testException.getClass().getSimpleName() + ": " +
        testException.getMessage();
  }

  private static final class MockOzoneManagerProtocol
      implements OzoneManagerProtocolPB {

    private final String omNodeId;
    // Exception to throw when submitMockRequest is called
    private final Supplier<Exception> exception;

    private MockOzoneManagerProtocol(String nodeId, Supplier<Exception> ex) {
      omNodeId = nodeId;
      exception = ex;
    }

    @Override
    public OMResponse submitRequest(RpcController controller,
                                    OzoneManagerProtocolProtos.OMRequest request) throws ServiceException {
      throw new ServiceException("ServiceException of type " +
          exception.getClass() + " for " + omNodeId, exception.get());
    }
  }

  private final class MockSingleFailoverProxyProvider extends
      HadoopRpcSingleOMFailoverProxyProvider<OzoneManagerProtocolPB> {

    private final String omNodeId;

    private MockSingleFailoverProxyProvider(ConfigurationSource configuration, String omNodeId) throws IOException {
      super(configuration, null, null, omNodeId, OzoneManagerProtocolPB.class);
      this.omNodeId = omNodeId;
    }

    @Override
    protected ProxyInfo<OzoneManagerProtocolPB> createOMProxy() {
      return new ProxyInfo<>(new MockOzoneManagerProtocol(omNodeId, () -> testException), omNodeId);
    }

    @Override
    protected void loadOMClientConfig(ConfigurationSource conf, String omSvcId, String nodeId) throws IOException {
      setOmProxy(createOMProxy());
    }

    @Override
    protected Text computeDelegationTokenService() {
      return null;
    }
  }
}
