/*
   * Licensed to the Apache Software Foundation (ASF) under one
   * or more contributor license agreements.  See the NOTICE file
   * distributed with this work for additional information
   * regarding copyright ownership.  The ASF licenses this file
   * to you under the Apache License, Version 2.0 (the
   * "License"); you may not use this file except in compliance
   * with the License.  You may obtain a copy of the License at
   * <p>
   * http://www.apache.org/licenses/LICENSE-2.0
   * <p>
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */
package org.apache.hadoop.ozone.container.checksum;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CACertificateProvider;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import jakarta.annotation.Nonnull;
import org.apache.hadoop.ozone.container.common.helpers.TokenHelper;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;

import static org.apache.hadoop.ozone.container.common.helpers.TokenHelper.encode;
import static org.apache.hadoop.ozone.util.MetricUtil.captureLatencyNs;

/**
 * This class wraps necessary container-level rpc calls for container reconciliation.
 *   - GetContainerMerkleTree
 */
public class DNContainerOperationClient implements AutoCloseable {

  private static final Logger LOG =
      LoggerFactory.getLogger(DNContainerOperationClient.class);
  private final TokenHelper tokenHelper;
  private final XceiverClientManager xceiverClientManager;

  public DNContainerOperationClient(ConfigurationSource conf,
                                    CertificateClient certificateClient,
                                    SecretKeySignerClient secretKeyClient) throws IOException {
    this.tokenHelper = new TokenHelper(new SecurityConfig(conf), secretKeyClient);
    this.xceiverClientManager = createClientManager(conf, certificateClient);
  }

  @Nonnull
  private static XceiverClientManager createClientManager(
      ConfigurationSource conf, CertificateClient certificateClient)
      throws IOException {
    ClientTrustManager trustManager = null;
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      CACertificateProvider localCaCerts =
          () -> HAUtils.buildCAX509List(certificateClient, conf);
      CACertificateProvider remoteCacerts =
          () -> HAUtils.buildCAX509List(null, conf);
      trustManager = new ClientTrustManager(remoteCacerts, localCaCerts);
    }
    DatanodeConfiguration dnConf = conf.getObject(DatanodeConfiguration.class);
    return new XceiverClientManager(conf,
        new XceiverClientManager.XceiverClientManagerConfigBuilder()
            .setMaxCacheSize(dnConf.getContainerClientCacheSize())
            .setStaleThresholdMs(dnConf.getContainerClientCacheStaleThreshold())
            .build(), trustManager);
  }

  public XceiverClientManager getXceiverClientManager() {
    return xceiverClientManager;
  }

  public ContainerProtos.ContainerChecksumInfo getContainerChecksumInfo(long containerId, DatanodeDetails dn)
      throws IOException {
    XceiverClientSpi xceiverClient = this.xceiverClientManager.acquireClient(createSingleNodePipeline(dn));
    try {
      String containerToken = encode(tokenHelper.getContainerToken(
          ContainerID.valueOf(containerId)));
      ContainerProtos.GetContainerChecksumInfoResponseProto response =
          ContainerProtocolCalls.getContainerChecksumInfo(xceiverClient,
              containerId, containerToken);
      return ContainerProtos.ContainerChecksumInfo.parseFrom(response.getContainerChecksumInfo());
    } finally {
      this.xceiverClientManager.releaseClient(xceiverClient, false);
    }
  }

  public static Pipeline createSingleNodePipeline(DatanodeDetails dn) {
    return Pipeline.newBuilder()
        .setNodes(ImmutableList.of(dn))
        .setId(PipelineID.valueOf(dn.getUuid()))
        .setState(Pipeline.PipelineState.CLOSED)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE)).build();
  }

  @Override
  public void close() throws IOException {
    if (xceiverClientManager != null) {
      xceiverClientManager.close();
    }
  }
}
