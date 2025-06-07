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

package org.apache.hadoop.ozone.freon;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientCreator;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.x509.certificate.client.CACertificateProvider;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.util.PayloadUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

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
@MetaInfServices(FreonSubcommand.class)
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

  @Option(names = {"--ratis"},
      description = "if Ratis or grpc",
      defaultValue = "false")
  private boolean ratis = false;

  @CommandLine.ParentCommand
  private Freon freon;

  // empy constructor for picocli
  public DNRPCLoadGenerator() {
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
      configuration = freon.getOzoneConf();
    }
    ContainerOperationClient scmClient = new ContainerOperationClient(configuration);
    ContainerInfo containerInfo = scmClient.getContainer(containerID);

    List<Pipeline> pipelineList = scmClient.listPipelines();
    Pipeline pipeline = pipelineList.stream()
        .filter(p -> p.getId().equals(containerInfo.getPipelineID()))
        .findFirst()
        .orElse(null);
    // If GRPC, use STANDALONE pipeline
    if (!ratis) {
      if (!readOnly) {
        LOG.warn("Read only is not set to true for GRPC, setting it to true");
        readOnly = true;
      }
      pipeline = pipeline.copyForRead();
    }
    encodedContainerToken = scmClient.getEncodedContainerToken(containerID);
    XceiverClientFactory xceiverClientManager;
    OzoneManagerProtocolClientSideTranslatorPB omClient;
    if (OzoneSecurityUtil.isSecurityEnabled(configuration)) {
      omClient = createOmClient(configuration, null);
      CACertificateProvider caCerts = () -> omClient.getServiceInfo().provideCACerts();
      xceiverClientManager = new XceiverClientCreator(configuration,
          new ClientTrustManager(caCerts, null));
    } else {
      omClient = null;
      xceiverClientManager = new XceiverClientCreator(configuration);
    }
    clients = new ArrayList<>(numClients);
    for (int i = 0; i < numClients; i++) {
      clients.add(xceiverClientManager.acquireClient(pipeline));
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
        xceiverClientManager.releaseClient(client, false);
      }
      xceiverClientManager.close();
      scmClient.close();
    }
    return null;
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
      ContainerProtos.EchoResponseProto response =
          ContainerProtocolCalls.echo(clients.get(clientIndex), encodedContainerToken,
              containerID, payloadReqBytes, payloadRespSize, sleepTimeMs, readOnly);
      return null;
    });
  }
}


