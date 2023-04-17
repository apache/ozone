/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.admin.scm;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionInfo;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.common.helpers.DeletedBlocksTransactionInfoWrapper;
import org.apache.hadoop.hdds.server.JsonUtils;
import picocli.CommandLine;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Handler of getting expired deleted blocks from SCM side.
 */
@CommandLine.Command(
    name = "ls",
    description = "Print the failed DeletedBlocksTransaction(retry count = -1)",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class GetFailedDeletedBlocksTxnSubcommand extends ScmSubcommand {

  @CommandLine.ArgGroup(multiplicity = "1")
  private TransactionsOption group;

  static class TransactionsOption {
    @CommandLine.Option(names = {"-a", "--all"},
        description = "Get all the failed transactions.")
    private boolean getAll;

    @CommandLine.Option(names = {"-c", "--count"},
        defaultValue = "20",
        description = "Get at most the count number of the" +
            " failed transactions.")
    private int count;
  }

  @CommandLine.Option(names = {"-s", "--startTxId", "--start-tx-id"},
      defaultValue = "0",
      description = "The least transaction ID to start with, default 0." +
          " Only work with -c/--count")
  private long startTxId;

  @CommandLine.Option(names = {"-o", "--out"},
      description = "Print transactions into file in JSON format.")
  private String fileName;

  private static final int LIST_ALL_FAILED_TRANSACTIONS = -1;

  @Override
  public void execute(ScmClient client) throws IOException {
    List<DeletedBlocksTransactionInfo> response;
    int count = group.getAll ? LIST_ALL_FAILED_TRANSACTIONS : group.count;
    response = client.getFailedDeletedBlockTxn(count, startTxId);
    List<DeletedBlocksTransactionInfoWrapper> txns = response.stream()
        .map(DeletedBlocksTransactionInfoWrapper::fromProtobuf)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    String result = JsonUtils.toJsonStringWithDefaultPrettyPrinter(txns);
    if (fileName != null) {
      try (FileOutputStream f = new FileOutputStream(fileName)) {
        f.write(result.getBytes(StandardCharsets.UTF_8));
      }
    } else {
      System.out.println(result);
    }
  }
}
