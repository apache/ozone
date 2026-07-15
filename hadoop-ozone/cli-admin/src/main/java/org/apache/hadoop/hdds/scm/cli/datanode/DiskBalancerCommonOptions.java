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

import java.util.Collections;
import java.util.List;
import picocli.CommandLine;

/**
 * Common options for DiskBalancer commands.
 */
public class DiskBalancerCommonOptions {

  @CommandLine.Mixin
  private DatanodeParameters datanodeParameters;

  @CommandLine.Option(names = {"--in-service-datanodes"},
      description = "If set, the client will send DiskBalancer requests " +
          "to all available DataNodes in the HEALTHY and IN_SERVICE operational state.",
      required = false)
  private boolean inServiceDatanodes;

  @CommandLine.Option(names = {"--json"},
      description = "Format output as JSON",
      defaultValue = "false")
  private boolean json;

  public List<String> getDatanodes() {
    // Return empty list if datanodeParameters is null
    // when --in-service-datanodes is used without positional args
    return datanodeParameters != null ? datanodeParameters.getDatanodes() : Collections.emptyList();
  }

  public boolean isInServiceDatanodes() {
    return inServiceDatanodes;
  }

  public boolean isJson() {
    return json;
  }
}
