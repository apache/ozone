/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.admin.scm;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.cli.ScmOption;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.ozone.ha.ConfUtils;
import picocli.CommandLine;
import picocli.CommandLine.Mixin;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.Callable;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEFAULT_SERVICE_ID;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY;

/**
 * Handler of ozone admin om transfer command.
 */
@CommandLine.Command(
    name = "transfer",
    description = "Manually transfer the raft leadership to the target node.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class TransferScmLeaderSubCommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private ScmAdmin parent;

  @Mixin
  private ScmOption scmOption;

  @CommandLine.ArgGroup(multiplicity = "1")
  private TransferOption configGroup;

  static class TransferOption {
    @CommandLine.Option(
        names = {"-n", "--nodeId"},
        description = "The target Node Id of OM to transfer leadership." +
            " Will convert to host with default ratis port. E.g OM1."
    )
    private String scmNodeId;

    @CommandLine.Option(
        names = {"-o", "--host"},
        description = "The target leader Ozone Manager Host to " +
            "transfer leadership."
    )
    private String scmHost;

    @CommandLine.Option(names = {"-r", "--random"},
        description = "Randomly choose a follower to transfer leadership.")
    private boolean isRandom;
  }

  @Override
  public Void call() throws Exception {
    convertToHost();
    ScmClient client = scmOption.createScmClient(
        parent.getParent().getOzoneConf());
    client.transferLeadership(configGroup.scmHost, configGroup.isRandom);
    System.out.println("Transfer leadership success");
    return null;
  }

  /**
   * Convert the scmNodeId to IP:PORT format.
   *
   * @throws IOException
   */
  private void convertToHost() throws IOException {
    if (configGroup.scmNodeId != null) {
      String scmServiceId = scmOption.getScmServiceId();
      OzoneConfiguration conf = parent.getParent().getOzoneConf();
      if (scmServiceId == null) {
        scmServiceId = conf.getTrimmed(OZONE_SCM_DEFAULT_SERVICE_ID);
        if (scmServiceId == null) {
          Collection<String> serviceIds = conf.getTrimmedStringCollection(
              OZONE_SCM_SERVICE_IDS_KEY);
          if (serviceIds.size() == 1) {
            scmServiceId = serviceIds.iterator().next();
          } else {
            throw new IOException("Find " + serviceIds.size() + " " +
                OZONE_SCM_SERVICE_IDS_KEY + ". Please specify one with " +
                "-id/--service-id");
          }
        }
      }
      Objects.requireNonNull(scmServiceId);
      String rpcAddrKey = ConfUtils.addKeySuffixes(OZONE_SCM_ADDRESS_KEY,
          scmServiceId, configGroup.scmNodeId);
      String rpcAddrStr = conf.get(rpcAddrKey);
      if (rpcAddrStr == null || rpcAddrStr.isEmpty()) {
        throw new IllegalArgumentException("Configuration does not have any" +
            " value set for " + rpcAddrKey + ". SCM RPC Address should be" +
            " set for all nodes in an OM service.");
      }
      String ratisPortKey = ConfUtils.addKeySuffixes(OZONE_SCM_RATIS_PORT_KEY,
          scmServiceId, configGroup.scmNodeId);
      int ratisPort = conf.getInt(ratisPortKey, OZONE_SCM_RATIS_PORT_DEFAULT);
      // Remove possible RPC port
      if (rpcAddrStr.contains(":")) {
        rpcAddrStr = rpcAddrStr.split(":")[0];
      }
      configGroup.scmHost = rpcAddrStr.concat(":")
          .concat(String.valueOf(ratisPort));
    }
  }
}
