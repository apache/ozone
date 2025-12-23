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

package org.apache.hadoop.ozone.admin.om;

import java.io.PrintStream;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Handler of ozone admin om lifecycle suspend command.
 */
@Command(
    name = "suspend",
    description = "Suspend Lifecycle Service. Use 'resume' command to resume it, " +
        "or it will be re-enabled after OM restarts based on the configuration",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class LifecycleSuspendSubCommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private LifecycleSubCommand parent;

  @CommandLine.Option(
      names = {"-id", "--service-id"},
      description = "Ozone Manager Service ID"
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"-host", "--service-host"},
      description = "Ozone Manager Host"
  )
  private String omHost;

  @Override
  public Void call() throws Exception {
    try (OzoneManagerProtocol ozoneManagerClient =
             parent.getParent().createOmClient(omServiceId, omHost, false)) {
      ozoneManagerClient.suspendLifecycleService();
      output();
    }
    return null;
  }

  protected void output() {
    PrintStream out = out();
    out.println("========================================");
    out.println("Lifecycle Service has been suspended.");
    out.println("Use 'ozone admin om lifecycle resume' to resume it,");
    out.println("or it will be re-enabled after OM restarts based on the configuration.");
    out.println("========================================");
  }

  protected PrintStream out() {
    return System.out;
  }
}

