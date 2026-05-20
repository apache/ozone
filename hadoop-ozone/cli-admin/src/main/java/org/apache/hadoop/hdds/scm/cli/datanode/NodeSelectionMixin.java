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

import com.google.common.base.Strings;
import picocli.CommandLine;

/**
 * Picocli mixin providing standardized datanode selection options for consistent CLI usage across commands.
 */
public class NodeSelectionMixin {

  @CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
  private Selection selection = new Selection();

  //Precedence order: --node-id > --id (deprecated) > --uuid (deprecated).
  public String getNodeId() {
    return !Strings.isNullOrEmpty(selection.nodeId) ? selection.nodeId :
        !Strings.isNullOrEmpty(selection.id) ? selection.id :
            !Strings.isNullOrEmpty(selection.uuid) ? selection.uuid : "";
  }

  public String getHostname() {
    return selection.hostname;
  }

  public String getIp() {
    return selection.ip;
  }

  static class Selection {

    @CommandLine.Option(names = "--node-id", description = "UUID of the datanode.", defaultValue = "")
    private String nodeId;

    @Deprecated
    @CommandLine.Option(names = "--id", description = "UUID of the datanode.", defaultValue = "", hidden = true)
    private String id;

    @Deprecated
    @CommandLine.Option(names = "--uuid", description = "UUID of the datanode.", defaultValue = "", hidden = true)
    private String uuid;

    @CommandLine.Option(names = "--hostname", description = "Hostname of the datanode. " +
        "Note: not supported for decommission status command", defaultValue = "")
    private String hostname;

    @CommandLine.Option(names = "--ip", description = "IP address of the datanode", defaultValue = "")
    private String ip;
  }
}
