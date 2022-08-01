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
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EchoRPCResponse;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Utility to generate RPC request to OM with or without payload.
 */
@Command(name = "om-rpc-load",
        aliases = "rpcl",
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
  private OzoneManagerProtocolClientSideTranslatorPB client;
  private byte[] payloadReq;
  private int payloadRespSize;
  @Option(names = {"-plrq", "--payload-req"},
          description =
                  "Specifies the size of payload in KB in RPC request. " +
                          "Max size is 2097151 KB",
          defaultValue = "1")
  private int payloadReqSizeKB = 1;

  @Option(names = {"-plrp", "--payload-resp"},
          description =
                  "Specifies the size of payload in KB in RPC response. " +
                          "Max size is 2097151 KB",
          defaultValue = "1")
  private int payloadRespSizeKB = 1;

  @Option(names = {"-erq", "--empty-req"},
          description =
                  "Specifies whether the payload of request is empty or not",
          defaultValue = "False")
  private boolean isEmptyReq = false;

  @Option(names = {"-erp", "--empty-resp"},
          description =
                  "Specifies whether the payload of response is empty or not",
          defaultValue = "False")
  private boolean isEmptyResp = false;

  @Override
  public Void call() throws Exception {
    if (payloadReqSizeKB < 0 || payloadRespSizeKB < 0) {
      throw new IllegalArgumentException(
              "RPC request or response payload can't be negative value."
      );
    }

    configuration = createOzoneConfiguration();
    client = createOmClient(configuration, null);
    init();
    if (isEmptyReq) {
      payloadReq = null;
    } else {
      //To avoid integer overflow, we cap the payload by max 2048000 KB
      int payloadReqSize = Math.min(
              Math.toIntExact(payloadReqSizeKB *
                      RPC_PAYLOAD_MULTIPLICATION_FACTOR),
              MAX_SIZE_KB);
      payloadReq = RandomUtils.nextBytes(payloadReqSize);
    }
    if (isEmptyResp) {
      payloadRespSize = 0;
    } else {
      //To avoid integer overflow, we cap the payload by max 2048000 KB
      payloadRespSize = Math.min(
              Math.toIntExact(payloadRespSizeKB *
                      RPC_PAYLOAD_MULTIPLICATION_FACTOR),
              MAX_SIZE_KB);
    }
    timer = getMetrics().timer("rpc-payload");
    for (int i = 0; i < getThreadNo(); i++) {
      runTests(this::sendRPCReq);
    }
    printReport();
    return null;
  }
  private void sendRPCReq(long l) throws Exception {
    timer.time(() -> {
      EchoRPCResponse resp = client.echoRPCReq(payloadReq,
              payloadRespSize);
      return resp;
    });
  }
}


