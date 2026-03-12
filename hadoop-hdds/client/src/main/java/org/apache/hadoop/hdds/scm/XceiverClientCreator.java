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

package org.apache.hadoop.hdds.scm;

import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneSecurityUtil;

/**
 * Factory for XceiverClientSpi implementations.  Client instances are not cached.
 */
public class XceiverClientCreator implements XceiverClientFactory {
  private static ErrorInjector errorInjector;

  private final ConfigurationSource conf;
  private final boolean topologyAwareRead;
  private final ClientTrustManager trustManager;
  private final boolean securityEnabled;

  public XceiverClientCreator(ConfigurationSource conf) {
    this(conf, null);
  }

  public XceiverClientCreator(ConfigurationSource conf, ClientTrustManager trustManager) {
    this.conf = conf;
    this.securityEnabled = OzoneSecurityUtil.isSecurityEnabled(conf);
    topologyAwareRead = conf.getBoolean(
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY,
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_DEFAULT);
    this.trustManager = trustManager;
    if (securityEnabled) {
      Objects.requireNonNull(trustManager, "trustManager == null");
    }
  }

  public static void enableErrorInjection(ErrorInjector injector) {
    errorInjector = injector;
  }

  public boolean isSecurityEnabled() {
    return securityEnabled;
  }

  protected XceiverClientSpi newClient(Pipeline pipeline) throws IOException {
    XceiverClientSpi client;
    switch (pipeline.getType()) {
    case RATIS:
      client = XceiverClientRatis.newXceiverClientRatis(pipeline, conf, trustManager, errorInjector);
      break;
    case STAND_ALONE:
      client = new XceiverClientGrpc(pipeline, conf, trustManager);
      break;
    case EC:
      client = new ECXceiverClientGrpc(pipeline, conf, trustManager);
      break;
    case CHAINED:
    default:
      throw new IOException("not implemented " + pipeline.getType());
    }
    try {
      client.connect();
    } catch (Exception e) {
      throw new IOException(e);
    }
    return client;
  }

  @Override
  public XceiverClientSpi acquireClient(Pipeline pipeline) throws IOException {
    return acquireClient(pipeline, false);
  }

  @Override
  public void releaseClient(XceiverClientSpi xceiverClient, boolean invalidateClient) {
    releaseClient(xceiverClient, invalidateClient, false);
  }

  @Override
  public XceiverClientSpi acquireClientForReadData(Pipeline pipeline) throws IOException {
    return acquireClient(pipeline);
  }

  @Override
  public void releaseClientForReadData(XceiverClientSpi xceiverClient, boolean invalidateClient) {
    releaseClient(xceiverClient, invalidateClient, topologyAwareRead);
  }

  @Override
  public XceiverClientSpi acquireClient(Pipeline pipeline, boolean topologyAware) throws IOException {
    return newClient(pipeline);
  }

  @Override
  public void releaseClient(XceiverClientSpi xceiverClient, boolean invalidateClient, boolean topologyAware) {
    IOUtils.closeQuietly(xceiverClient);
  }

  @Override
  public void close() throws Exception {
    // clients are not tracked, closing each client is the responsibility of users of this class
  }
}
