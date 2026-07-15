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
import com.google.common.base.Preconditions;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.util.PayloadUtils;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Utility to generate RPC request to OM with or without payload.
 */
@Command(name = "om-echo",
        aliases = "ome",
        description =
                "Generate echo RPC request to the OM " +
                        "with or without payload. " +
                        "Max payload size is 2097151 KB",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true,
        showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class OmRPCLoadGenerator extends BaseFreonGenerator
        implements Callable<Void> {

  private Timer timer;
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

  @Option(names = {"--ratis"},
      description = "Write to Ratis log, skip flag for read-only EchoRPC " +
          "request")
  private boolean writeToRatis = false;

  @Override
  public Void call() throws Exception {
    Preconditions.checkArgument(payloadReqSizeKB >= 0,
            "OM echo request payload size should be positive value or zero.");
    Preconditions.checkArgument(payloadRespSizeKB >= 0,
            "OM echo response payload size should be positive value or zero.");

    OzoneConfiguration configuration = createOzoneConfiguration();
    clients = new OzoneManagerProtocolClientSideTranslatorPB[clientsCount];
    for (int i = 0; i < clientsCount; i++) {
      clients[i] = createOmClient(configuration, null);
    }

    init();
    payloadReqBytes = PayloadUtils.generatePayload(payloadSizeInBytes(payloadReqSizeKB));
    payloadRespSize = payloadSizeInBytes(payloadRespSizeKB);
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

  private int payloadSizeInBytes(int payloadSizeKB) {
    return payloadSizeKB > 0 ? payloadSizeKB * 1024 : 0;
  }

  private void sendRPCReq(long l) throws Exception {
    timer.time(() -> {
      clients[(int) (l % clientsCount)].echoRPCReq(payloadReqBytes,
              payloadRespSize, writeToRatis);
      return null;
    });
  }
}


