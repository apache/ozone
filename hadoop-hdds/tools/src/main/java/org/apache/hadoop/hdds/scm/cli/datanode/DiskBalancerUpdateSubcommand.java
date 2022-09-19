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
package org.apache.hadoop.hdds.scm.cli.datanode;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Handler to update disk balancer configuration.
 */
@Command(
    name = "update",
    description = "Update DiskBalancer Configuration",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class DiskBalancerUpdateSubcommand extends ScmSubcommand {

  @Option(names = {"-t", "--threshold"},
      description = "Percentage deviation from average utilization of " +
          "the disks after which a datanode will be rebalanced (for " +
          "example, '10' for 10%%).")
  private Optional<Double> threshold;

  @Option(names = {"-b", "--bandwidthInMB"},
      description = "Maximum bandwidth for DiskBalancer per second.")
  private Optional<Long> bandwidthInMB;

  @Option(names = {"-p", "--parallelThread"},
      description = "Max parallelThread for DiskBalancer.")
  private Optional<Integer> parallelThread;

  @CommandLine.Mixin
  private DiskBalancerCommonOptions commonOptions =
      new DiskBalancerCommonOptions();

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    if (!commonOptions.check()) {
      return;
    }
    List<DatanodeAdminError> errors =
        scmClient.updateDiskBalancerConfiguration(threshold, bandwidthInMB,
            parallelThread, commonOptions.getHosts().size() == 0 ?
                Optional.empty() : Optional.of(commonOptions.getHosts()));

    System.out.println("Update DiskBalancer Configuration on datanode(s):\n" +
        (commonOptions.isAllHosts() ? "All datanodes" : String.join("\n",
            commonOptions.getHosts())));

    if (errors.size() > 0) {
      for (DatanodeAdminError error : errors) {
        System.err.println("Error: " + error.getHostname() + ": "
            + error.getError());
      }
      throw new IOException(
          "Some nodes could not update DiskBalancer.");
    }
  }

  @VisibleForTesting
  public void setAllHosts(boolean allHosts) {
    this.commonOptions.setAllHosts(allHosts);
  }
}