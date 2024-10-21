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
package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientShortCircuit;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.storage.DomainSocketFactory;
import org.apache.hadoop.hdds.security.x509.certificate.client.CACertificateProvider;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.util.PayloadUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hdds.client.ReplicationConfig.getLegacyFactor;

/**
 * Utility to generate RPC request to DN.
 */
@Command(name = "dn-echo",
        aliases = "dne",
        description =
                "Generate echo RPC request to DataNode",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true,
        showDefaultValues = true)
public class DNRPCLoadGenerator extends BaseFreonGenerator
        implements Callable<Void> {
  private static final Logger LOG =
      LoggerFactory.getLogger(DNRPCLoadGenerator.class);
  private static final int RPC_PAYLOAD_MULTIPLICATION_FACTOR = 1024;
  private static final int MAX_SIZE_KB = 2097151;
  private Timer timer;
  private OzoneConfiguration configuration;
  private ByteString payloadReqBytes;
  private int payloadRespSize;
  private List<XceiverClientSpi> clients;
  private String encodedContainerToken;
  private final AtomicLong callId = new AtomicLong(0);
  private final ByteString clientId = ByteString.copyFrom(this.getClass().getSimpleName().getBytes());
  private boolean shortCircuitRead = false;
  @Option(names = {"--payload-req"},
          description =
                  "Specifies the size of payload in KB in RPC request. ",
          defaultValue = "0")
  private int payloadReqSizeKB = 0;

  @Option(names = {"--payload-resp"},
          description =
                  "Specifies the size of payload in KB in RPC response. ",
          defaultValue = "0")
  private int payloadRespSizeKB = 0;

  @Option(names = {"--container-id"},
      description = "Send echo to DataNodes associated with this container")
  private long containerID;

  @Option(names = {"--sleep-time-ms"},
      description = "Let DataNode to pause for a duration (in milliseconds) for each request",
      defaultValue = "0")
  private int sleepTimeMs = 0;

  @Option(names = {"--clients"},
      description = "number of xceiver clients",
      defaultValue = "1")
  private int numClients = 1;

  @Option(names = {"--read-only"},
      description = "if Ratis, read only or not",
      defaultValue = "false")
  private boolean readOnly = false;

  @Option(names = {"--channel-type"},
      description = "if Ratis, grpc or short-circuit",
      defaultValue = "grpc")
  private String type = "grpc";

  @CommandLine.ParentCommand
  private Freon freon;

  // empty constructor for picocli
  DNRPCLoadGenerator() {
  }

  @VisibleForTesting
  DNRPCLoadGenerator(OzoneConfiguration ozoneConfiguration) {
    this.configuration = ozoneConfiguration;
  }

  @Override
  public Void call() throws Exception {
    Preconditions.checkArgument(payloadReqSizeKB >= 0,
        "OM echo request payload size should be positive value or zero.");
    Preconditions.checkArgument(payloadRespSizeKB >= 0,
        "OM echo response payload size should be positive value or zero.");

    if (configuration == null) {
      configuration = freon.createOzoneConfiguration();
    }

    if (type.equalsIgnoreCase("short-circuit")) {
      boolean shortCircuit = configuration.getBoolean(OzoneClientConfig.OZONE_READ_SHORT_CIRCUIT,
          OzoneClientConfig.OZONE_READ_SHORT_CIRCUIT_DEFAULT);
      if (!shortCircuit) {
        LOG.error("Short-circuit is not enabled");
        return null;
      }
      DomainSocketFactory domainSocketFactory = DomainSocketFactory.getInstance(configuration);
      if (domainSocketFactory != null && domainSocketFactory.isServiceReady()) {
        shortCircuitRead = true;
      } else {
        LOG.error("Short-circuit read is enabled but service is not ready");
        return null;
      }
    }
    ContainerOperationClient scmClient = new ContainerOperationClient(configuration);
    encodedContainerToken = scmClient.getEncodedContainerToken(containerID);
    XceiverClientFactory xceiverClientManager;
    OzoneManagerProtocolClientSideTranslatorPB omClient;
    if (OzoneSecurityUtil.isSecurityEnabled(configuration)) {
      omClient = createOmClient(configuration, null);
      CACertificateProvider caCerts = () -> omClient.getServiceInfo().provideCACerts();
      xceiverClientManager = new XceiverClientManager(configuration,
          configuration.getObject(XceiverClientManager.ScmClientConfig.class),
          new ClientTrustManager(caCerts, null));
    } else {
      omClient = null;
      xceiverClientManager = new XceiverClientManager(configuration);
    }
    clients = new ArrayList<>(numClients);
    for (int i = 0; i < numClients; i++) {
      Pipeline pipeline = getPipeline(scmClient, containerID);
      XceiverClientSpi client = shortCircuitRead ? xceiverClientManager.acquireClientForReadData(pipeline, true) :
          xceiverClientManager.acquireClient(pipeline);
      if (shortCircuitRead) {
        if (!(client instanceof XceiverClientShortCircuit)) {
          LOG.error("Short-circuit is enabled while client is of type {}", client.getClass().getSimpleName());
          clients.forEach(c -> xceiverClientManager.releaseClient(c, false));
          xceiverClientManager.close();
          scmClient.close();
          return null;
        }
      }
      clients.add(client);
    }

    init();
    payloadReqBytes = PayloadUtils.generatePayloadProto3(payloadSizeInBytes(payloadReqSizeKB));
    payloadRespSize = calculateMaxPayloadSize(payloadRespSizeKB);
    timer = getMetrics().timer("rpc-payload");
    try {
      runTests(this::sendRPCReq);
    } finally {
      if (omClient != null) {
        omClient.close();
      }
      for (XceiverClientSpi client : clients) {
        xceiverClientManager.releaseClient(client, true);
      }
      clients = null;
      xceiverClientManager.close();
      scmClient.close();
    }
    return null;
  }

  private Pipeline getPipeline(ContainerOperationClient scmClient, long containerId) throws IOException {
    ContainerWithPipeline containerInfo = scmClient.getContainerWithPipeline(containerId);

    List<Pipeline> pipelineList = scmClient.listPipelines();
    Pipeline pipeline = pipelineList.stream()
        .filter(p -> p.getId().equals(containerInfo.getPipeline().getId()))
        .findFirst()
        .orElse(null);
    // If GRPC or Short-circuit, use STANDALONE pipeline
    if (!type.equalsIgnoreCase("ratis")) {
      if (!readOnly) {
        LOG.warn("Read only is not set to true for grpc/short-circuit, setting it to true");
        readOnly = true;
      }
      if (pipeline == null) {
        pipeline = containerInfo.getPipeline();
      }
      pipeline = Pipeline.newBuilder(pipeline)
          .setReplicationConfig(StandaloneReplicationConfig.getInstance(
              getLegacyFactor(pipeline.getReplicationConfig())))
          .build();
    }
    return pipeline;
  }

  private int calculateMaxPayloadSize(int payloadSizeKB) {
    if (payloadSizeKB > 0) {
      return Math.min(
              Math.toIntExact((long)payloadSizeKB *
                      RPC_PAYLOAD_MULTIPLICATION_FACTOR),
              MAX_SIZE_KB);
    }
    return 0;
  }

  private int payloadSizeInBytes(int payloadSizeKB) {
    return payloadSizeKB > 0 ? payloadSizeKB * 1024 : 0;
  }

  private void sendRPCReq(long l) throws Exception {
    timer.time(() -> {
      int clientIndex = (numClients == 1) ? 0 : (int)l % numClients;
      ContainerProtos.EchoResponseProto response = shortCircuitRead ?
          ContainerProtocolCalls.echo(clients.get(clientIndex), encodedContainerToken,
              containerID, payloadReqBytes, payloadRespSize, sleepTimeMs, readOnly,
              clientId, callId.incrementAndGet(), false) :
          ContainerProtocolCalls.echo(clients.get(clientIndex), encodedContainerToken,
              containerID, payloadReqBytes, payloadRespSize, sleepTimeMs, readOnly);
      return null;
    });
  }
}


