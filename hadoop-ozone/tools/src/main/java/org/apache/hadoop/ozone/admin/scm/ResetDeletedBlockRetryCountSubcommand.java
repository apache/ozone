/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.admin.scm;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.common.helpers.DeletedBlocksTransactionInfoWrapper;
import org.apache.hadoop.hdds.server.JsonUtils;
import picocli.CommandLine;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Handler of resetting expired deleted blocks from SCM side.
 */
@CommandLine.Command(
    name = "reset",
    description = "Reset the retry count of failed DeletedBlocksTransaction",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ResetDeletedBlockRetryCountSubcommand extends ScmSubcommand {

  @CommandLine.ArgGroup(multiplicity = "1")
  private TransactionsOption group;

  static class TransactionsOption {
    @CommandLine.Option(names = {"-a", "--all"},
        description = "Reset all expired deleted block transaction retry" +
            " count from -1 to 0.")
    private boolean resetAll;

    @CommandLine.Option(names = {"-l", "--list"},
        split = ",",
        paramLabel = "txID",
        description = "Reset the only given deletedBlock transaction ID" +
            " list. Example: 100,101,102.(Separated by ',')")
    private List<Long> txList;

    @CommandLine.Option(names = {"-i", "--in"},
        description = "Use file as input, need to be JSON Array format and " +
            "contains multi \"txID\" key. Example: [{\"txID\":1},{\"txID\":2}]")
    private String fileName;
  }

  @Override
  public void execute(ScmClient client) throws IOException {
    int count;
    if (group.resetAll) {
      count = client.resetDeletedBlockRetryCount(new ArrayList<>());
    } else if (group.fileName != null) {
      List<Long> txIDs;
      try (InputStream in = new FileInputStream(group.fileName);
           Reader fileReader = new InputStreamReader(in,
               StandardCharsets.UTF_8)) {
        DeletedBlocksTransactionInfoWrapper[] txns = JsonUtils.readFromReader(fileReader,
            DeletedBlocksTransactionInfoWrapper[].class);
        txIDs = Arrays.stream(txns)
            .map(DeletedBlocksTransactionInfoWrapper::getTxID)
            .sorted()
            .distinct()
            .collect(Collectors.toList());
        System.out.println("Num of loaded txIDs: " + txIDs.size());
        if (!txIDs.isEmpty()) {
          System.out.println("The first loaded txID: " + txIDs.get(0));
          System.out.println("The last loaded txID: " +
              txIDs.get(txIDs.size() - 1));
        }
      } catch (IOException ex) {
        final String message = "Failed to parse the file " + group.fileName + ": " + ex.getMessage();
        System.out.println(message);
        throw new IOException(message, ex);
      }

      count = client.resetDeletedBlockRetryCount(txIDs);
    } else {
      if (group.txList == null || group.txList.isEmpty()) {
        System.out.println("TransactionId list should not be empty");
        return;
      }
      count = client.resetDeletedBlockRetryCount(group.txList);
    }
    System.out.println("Reset " + count + " deleted block transactions in" +
        " SCM.");
  }
}
