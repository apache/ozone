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

import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Common options for DiskBalancer commands.
 */
public class DiskBalancerCommonOptions {

  @CommandLine.Option(names = {"-a", "--all"},
      description = "Run commands on all datanodes.")
  private boolean allHosts;

  @CommandLine.Option(names = {"-d", "--datanodes"},
      description = "Run commands on specific datanodes, the content can be " +
          "a list of hostnames or IPs. " +
          "Examples: hostname1,hostname2,hostname3 or ip1,ip2,ip3")
  private List<String> datanodes = new ArrayList<>();

  /**
   * Check the common options of DiskBalancerCommand.
   * @return if the check passed
   */
  public boolean check() {
    if (datanodes.size() == 0 && !allHosts) {
      System.out.println("Datanode not specified. Please specify at least " +
          "one datanode or use \"-a(--all)\" to start diskBalancer " +
          "on all datanodes");
      return false;
    }
    if (datanodes.size() != 0 && allHosts) {
      System.out.println("Invalid option selection. " +
          "Use either \"-a(--all)\" or \"-d(--datanodes)\".");
      return false;
    }
    return true;
  }

  public String getHostString() {
    return isAllHosts() ? "All datanodes" : String.join("\n", getDatanodes());
  }

  public Optional<List<String>> getSpecifiedDatanodes() {
    return getDatanodes().size() == 0 ?
        Optional.empty() : Optional.of(getDatanodes());
  }

  public boolean isAllHosts() {
    return allHosts;
  }

  public void setAllHosts(boolean allHosts) {
    this.allHosts = allHosts;
  }

  public List<String> getDatanodes() {
    return datanodes;
  }

  public void setDatanodes(List<String> datanodes) {
    this.datanodes = datanodes;
  }
}
