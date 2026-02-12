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

package org.apache.hadoop.ozone.om.failover;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.ha.HadoopRpcOMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase;
import org.apache.hadoop.ozone.om.ha.OMProxyInfo;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.AccessControlException;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

/**
 * Tests OM failover protocols using a Mock Failover provider and a Mock OM
 * Protocol.
 */
public class TestOMFailovers {

  private ConfigurationSource conf = new OzoneConfiguration();
  private Exception testException;

  @Test
  public void testAccessControlExceptionFailovers() throws Exception {

    testException = new AccessControlException();

    GenericTestUtils.setLogLevel(OMFailoverProxyProviderBase.class, Level.DEBUG);
    LogCapturer logCapturer = LogCapturer.captureLogs(OMFailoverProxyProviderBase.class);

    MockFailoverProxyProvider failoverProxyProvider =
        new MockFailoverProxyProvider(conf);

    OzoneManagerProtocolPB proxy = (OzoneManagerProtocolPB) RetryProxy
        .create(OzoneManagerProtocolPB.class, failoverProxyProvider,
            failoverProxyProvider.getRetryPolicy(
                OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT));

    ServiceException serviceException = assertThrows(ServiceException.class,
        () -> proxy.submitRequest(null, null));

    // Request should try all OMs one be one and fail when the last OM also
    // throws AccessControlException.
    assertThat(serviceException).hasCauseInstanceOf(AccessControlException.class)
        .hasMessageStartingWith("ServiceException of type class org.apache.hadoop.security.AccessControlException");
    assertThat(logCapturer.getOutput()).contains(getRetryProxyDebugMsg("om1"));
    assertThat(logCapturer.getOutput()).contains(getRetryProxyDebugMsg("om2"));
    assertThat(logCapturer.getOutput()).contains(getRetryProxyDebugMsg("om3"));
  }

  private String getRetryProxyDebugMsg(String omNodeId) {
    return "RetryProxy: OM " + omNodeId + ": AccessControlException: " +
        "Permission denied.";
  }

  private static final class MockOzoneManagerProtocol
      implements OzoneManagerProtocolPB {

    private final InetSocketAddress omAddress;
    // Exception to throw when submitMockRequest is called
    private final Exception exception;

    private MockOzoneManagerProtocol(InetSocketAddress address, Exception ex) {
      omAddress = address;
      exception = ex;
    }

    @Override
    public OMResponse submitRequest(RpcController controller,
        OzoneManagerProtocolProtos.OMRequest request) throws ServiceException {
      throw new ServiceException("ServiceException of type " +
          exception.getClass() + " for " + omAddress, exception);
    }
  }

  private final class MockFailoverProxyProvider extends HadoopRpcOMFailoverProxyProvider<OzoneManagerProtocolPB> {

    private MockFailoverProxyProvider(ConfigurationSource configuration)
        throws IOException {
      super(configuration, null, null, OzoneManagerProtocolPB.class);
    }

    @Override
    protected OzoneManagerProtocolPB createOMProxy(InetSocketAddress omAddress) throws IOException {
      return new MockOzoneManagerProtocol(omAddress, testException);
    }

    @Override
    protected List<OMProxyInfo<OzoneManagerProtocolPB>> initOmProxiesFromConfigs(ConfigurationSource config,
        String omSvcId) {
      final List<OMProxyInfo<OzoneManagerProtocolPB>> omProxyInfos = new ArrayList<>();

      for (int i = 1; i <= 3; i++) {
        String nodeId = "om" + i;
        omProxyInfos.add(OMProxyInfo.newInstance(null, omSvcId, nodeId, "127.0.0.1:986" + i));
      }
      return omProxyInfos;
    }

    @Override
    protected Text computeDelegationTokenService() {
      return null;
    }
  }
}
