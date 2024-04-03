/**
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

package org.apache.hadoop.ozone.admin.om;

import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

/**
 * Handler of ozone admin om fetch-topology-tree command.
 */
@CommandLine.Command(
    name = "fetch-topology-tree",
    description = "CLI command for OM to force fetch the latest network " +
        "topology tree information from SCM.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class FetchNetworkTopologyTreeSubCommand implements Callable<Void> {
  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(
      names = {"-id", "--service-id"},
      description = "Ozone Manager Service ID",
      required = false
  )
  private String omServiceId;

  @Override
  public Void call() throws Exception {
    try (OzoneManagerProtocol client = parent.createOmClient(omServiceId)) {
      boolean status = false;
      try {
        status = client.refetchNetworkTopologyTree();
      } catch (IOException e) {
        System.err.println("Force fetching network topology tree information " +
            "has failed: " + e.getMessage());
      }
      if (status) {
        System.out.println(
            "Force fetching network topology tree information " +
                "is complete.");
      }
    }
    return null;
  }
}
