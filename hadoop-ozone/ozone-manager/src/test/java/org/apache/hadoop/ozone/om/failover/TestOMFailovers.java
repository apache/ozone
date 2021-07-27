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

package org.apache.hadoop.ozone.om.failover;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.OMProxyInfo;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.AccessControlException;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.event.Level;

/**
 * Tests OM failover protocols using a Mock Failover provider and a Mock OM
 * Protocol.
 */
public class TestOMFailovers {

  private ConfigurationSource conf = new OzoneConfiguration();
  private Exception testException;

  @Test
  public void testAccessContorlExceptionFailovers() throws Exception {

    testException = new AccessControlException();

    GenericTestUtils.setLogLevel(OMFailoverProxyProvider.LOG, Level.DEBUG);
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(OMFailoverProxyProvider.LOG);

    MockFailoverProxyProvider failoverProxyProvider =
        new MockFailoverProxyProvider(conf);

    OzoneManagerProtocolPB proxy = (OzoneManagerProtocolPB) RetryProxy
        .create(OzoneManagerProtocolPB.class, failoverProxyProvider,
            failoverProxyProvider.getRetryPolicy(
                OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_DEFAULT));

    try {
      proxy.submitRequest(null, null);
      Assert.fail("Request should fail with AccessControlException");
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof ServiceException);

      // Request should try all OMs one be one and fail when the last OM also
      // throws AccessControlException.
      GenericTestUtils.assertExceptionContains("ServiceException of " +
          "type class org.apache.hadoop.security.AccessControlException for " +
          "om3", ex);
      Assert.assertTrue(ex.getCause() instanceof AccessControlException);

      Assert.assertTrue(
          logCapturer.getOutput().contains(getRetryProxyDebugMsg("om1")));
      Assert.assertTrue(
          logCapturer.getOutput().contains(getRetryProxyDebugMsg("om2")));
      Assert.assertTrue(
          logCapturer.getOutput().contains(getRetryProxyDebugMsg("om3")));
    }
  }

  private String getRetryProxyDebugMsg(String omNodeId) {
    return "RetryProxy: OM " + omNodeId + ": AccessControlException: " +
        "Permission denied.";
  }

  private static final class MockOzoneManagerProtocol
      implements OzoneManagerProtocolPB {

    private final String omNodeId;
    // Exception to throw when submitMockRequest is called
    private final Exception exception;

    private MockOzoneManagerProtocol(String nodeId, Exception ex) {
      omNodeId = nodeId;
      exception = ex;
    }

    @Override
    public OMResponse submitRequest(RpcController controller,
        OzoneManagerProtocolProtos.OMRequest request) throws ServiceException {
      throw new ServiceException("ServiceException of type " +
          exception.getClass() + " for "+ omNodeId, exception);
    }
  }

  private final class MockFailoverProxyProvider
      extends OMFailoverProxyProvider {

    private MockFailoverProxyProvider(ConfigurationSource configuration)
        throws IOException {
      super(configuration, null, null, OzoneManagerProtocolPB.class);
    }

    @Override
    protected ProxyInfo createOMProxy(String nodeId) {
      ProxyInfo proxyInfo = new ProxyInfo<>(new MockOzoneManagerProtocol(nodeId,
          testException), nodeId);
      getOMProxyMap().put(nodeId, proxyInfo);
      return proxyInfo;
    }

    @Override
    protected void loadOMClientConfigs(ConfigurationSource config,
        String omSvcId) {
      HashMap<String, ProxyInfo<OzoneManagerProtocolPB>> omProxies =
          new HashMap<>();
      HashMap<String, OMProxyInfo> omProxyInfos = new HashMap<>();
      ArrayList<String> omNodeIDList = new ArrayList<>();

      for (int i = 1; i <= 3; i++) {
        String nodeId = "om" + i;
        omProxies.put(nodeId, null);
        omProxyInfos.put(nodeId, null);
        omNodeIDList.add(nodeId);
      }
      setProxiesForTesting(omProxies, omProxyInfos, omNodeIDList);
    }

    @Override
    protected Text computeDelegationTokenService() {
      return null;
    }
  }
}
