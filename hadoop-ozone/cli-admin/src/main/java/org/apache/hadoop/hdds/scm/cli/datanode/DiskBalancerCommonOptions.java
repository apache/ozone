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

package org.apache.hadoop.hdds.scm.cli.datanode;

import java.util.List;
import picocli.CommandLine;

/**
 * Common options for DiskBalancer commands.
 */
public class DiskBalancerCommonOptions {

  @CommandLine.Option(names = {"-d", "--datanodes"},
      description = "Datanode address(es) in the format <hostname[:port]> or <ip[:port]>. " +
          "Port is optional and defaults to 9858 (CLIENT_RPC port). " +
          "Multiple addresses can be comma-separated. " +
          "Examples: 'DN-1', 'DN-1:9858', '192.168.1.10', 'DN-1,DN-2:9999,DN-3'",
      required = false,
      split = ",")
  private List<String> datanodes;

  @CommandLine.Option(names = {"--in-service-datanodes"},
      description = "If set, the client will send DiskBalancer requests " +
          "to all available DataNodes in the HEALTHY and IN_SERVICE operational state.",
      required = false)
  private boolean inServiceDatanodes;

  public List<String> getDatanodes() {
    return datanodes;
  }

  public boolean isInServiceDatanodes() {
    return inServiceDatanodes;
  }
}
