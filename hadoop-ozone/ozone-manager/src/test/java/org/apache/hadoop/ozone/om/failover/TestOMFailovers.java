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
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.event.Level;

/**
 * Tests OM failover protocols using a Mock Failover provider and a Mock OM
 * Protocol.
 */
public class TestOMFailovers {

  ConfigurationSource conf = new OzoneConfiguration();
  Exception testException;

  @Test
  public void test1() throws Exception {

    testException = new AccessControlException();

    GenericTestUtils.setLogLevel(OMFailoverProxyProvider.LOG, Level.DEBUG);
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(OMFailoverProxyProvider.LOG);

    MockFailoverProxyProvider failoverProxyProvider =
        new MockFailoverProxyProvider(conf);

    OzoneManagerProtocolPB proxy = (OzoneManagerProtocolPB) RetryProxy
        .create(OzoneManagerProtocolPB.class, failoverProxyProvider,
            failoverProxyProvider.getRetryPolicy(9));

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

      logCapturer.getOutput().contains(getRetryProxyDebugMsg("om1"));
      logCapturer.getOutput().contains(getRetryProxyDebugMsg("om2"));
      logCapturer.getOutput().contains(getRetryProxyDebugMsg("om3"));
    }
  }

  private String getRetryProxyDebugMsg(String omNodeId) {
    return "RetryProxy: OM " + omNodeId +": AccessControlException: " +
        "Permission denied.";
  }

  private class MockOzoneManagerProtocol implements OzoneManagerProtocolPB {

    final String omNodeId;
    // Exception to throw when submitMockRequest is called
    final Exception exception;

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

  private class MockFailoverProxyProvider extends OMFailoverProxyProvider {

    private MockFailoverProxyProvider(ConfigurationSource configuration)
        throws IOException {
      super(configuration, null, null);
    }

    @Override
    protected void createOMProxyIfNeeded(ProxyInfo proxyInfo,
        String nodeId) {
      if (proxyInfo.proxy == null) {
        proxyInfo.proxy = new MockOzoneManagerProtocol(nodeId,
            testException);
      }
    }

    @Override
    protected void loadOMClientConfigs(ConfigurationSource config,
        String omSvcId) {
      this.omProxies = new HashMap<>();
      this.omProxyInfos = new HashMap<>();
      this.omNodeIDList = new ArrayList<>();

      for (int i = 1; i <= 3; i++) {
        String nodeId = "om" + i;
        omProxies.put(nodeId, new ProxyInfo<>(null, nodeId));
        omProxyInfos.put(nodeId, null);
        omNodeIDList.add(nodeId);
      }
    }

    @Override
    protected Text computeDelegationTokenService() {
      return null;
    }
  }
}
