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
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Utility to generate RPC request to OM with or without payload.
 */
@Command(name = "om-echo",
        aliases = "ome",
        description =
                "Generate echo RPC request to the OM " +
                        "with or without layload. " +
                        "Max payload size is 2097151 KB",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true,
        showDefaultValues = true)
public class OmRPCLoadGenerator extends BaseFreonGenerator
        implements Callable<Void> {

  private static final int RPC_PAYLOAD_MULTIPLICATION_FACTOR = 1024;
  private static final int MAX_SIZE_KB = 2097151;
  private Timer timer;
  private OzoneConfiguration configuration;
  private OzoneManagerProtocolClientSideTranslatorPB[] clients;
  private byte[] payloadReqBytes = new byte[0];
  private int payloadRespSize;
  @Option(names = {"--payload-req"},
          description =
                  "Specifies the size of payload in KB in RPC request. " +
                          "Max size is 2097151 KB",
          defaultValue = "0")
  private int payloadReqSizeKB = 0;

  @Option(names = {"--clients"},
      description =
          "Number of clients, defaults 1.",
      defaultValue = "1")
  private int clientsCount = 1;

  @Option(names = {"--payload-resp"},
          description =
                  "Specifies the size of payload in KB in RPC response. " +
                          "Max size is 2097151 KB",
          defaultValue = "0")
  private int payloadRespSizeKB = 0;
  @Override
  public Void call() throws Exception {
    Preconditions.checkArgument(payloadReqSizeKB >= 0,
            "OM echo request payload size should be positive value or zero.");
    Preconditions.checkArgument(payloadRespSizeKB >= 0,
            "OM echo response payload size should be positive value or zero.");

    configuration = createOzoneConfiguration();
    clients = new OzoneManagerProtocolClientSideTranslatorPB[clientsCount];
    for (int i = 0; i < clientsCount; i++) {
      clients[i] = createOmClient(configuration, null);
    }

    init();
    payloadReqBytes = RandomUtils.nextBytes(
            calculateMaxPayloadSize(payloadReqSizeKB));
    payloadRespSize = calculateMaxPayloadSize(payloadRespSizeKB);
    timer = getMetrics().timer("rpc-payload");
    try {
      runTests(this::sendRPCReq);
    } finally {
      for (int i = 0; i < clientsCount; i++) {
        if (clients[i] != null) {
          clients[i].close();
        }
      }
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

  private void sendRPCReq(long l) throws Exception {
    timer.time(() -> {
      clients[(int) (l % clientsCount)].echoRPCReq(payloadReqBytes,
              payloadRespSize);
      return null;
    });
  }
}


