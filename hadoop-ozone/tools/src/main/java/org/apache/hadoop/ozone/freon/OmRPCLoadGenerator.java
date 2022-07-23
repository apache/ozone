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

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Utility to generate RPC request to OM with or without payload.
 */
@Command(name = "om-rpc-load",
        aliases = "omrpcl",
        description =
                "Generate random RPC request to the OM " +
                        "with or without layload.",
        versionProvider = HddsVersionProvider.class,
        mixinStandardHelpOptions = true,
        showDefaultValues = true)
public class OmRPCLoadGenerator extends BaseFreonGenerator
        implements Callable<Void> {

  private static final Logger LOG =
          LoggerFactory.getLogger(OmRPCLoadGenerator.class);

  @Option(names = {"--payload"},
          description =
                  "Specifies the size of payload in bytes in each RPC request.",
          defaultValue = "1024")
  private int payloadSize = 1024;

  @Option(names = {"--empty-req"},
          description =
                  "Specifies whether the payload of request is empty or not",
          defaultValue = "False")
  private boolean isEmptyReq = false;

  @Option(names = {"--empty-resp"},
          description =
                  "Specifies whether the payload of response is empty or not",
          defaultValue = "False")
  private boolean isEmptyResp = false;

  @Override
  public Void call() throws Exception {
    init();
    int numOfThreads = getThreadNo();
    LOG.info("Number of Threads: {}", numOfThreads);
    LOG.info("RPC request payload size: {} bytes", payloadSize);
    LOG.info("Empty RPC request: {}", isEmptyReq);
    LOG.info("Empty RPC response: {}", isEmptyResp);

    for (int i = 0; i < numOfThreads; i++) {
      runTests(this::sendRPCReq);
    }
    return null;
  }
  private void sendRPCReq(long l) throws Exception {
    OzoneConfiguration configuration = createOzoneConfiguration();
    OzoneClient client = createOzoneClient(null, configuration);
    ServiceInfoEx serviceInfo = client.getProxy().
            getOzoneManagerClient().getServiceInfo();
    LOG.info("###########################################");
    LOG.info("###########################################");
    LOG.info(serviceInfo.toString());
    LOG.info("###########################################");
    LOG.info("###########################################");
  }

}


