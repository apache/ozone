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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetLifecycleServiceStatusResponse;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Handler of ozone admin om lifecycle status command.
 */
@Command(
    name = "status",
    description = "Check Lifecycle Service status",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class LifecycleStatusSubCommand implements Callable<Void> {

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
      GetLifecycleServiceStatusResponse lifecycleServiceStatus =
          ozoneManagerClient.getLifecycleServiceStatus();
      output(lifecycleServiceStatus);
    }
    return null;
  }

  protected void output(GetLifecycleServiceStatusResponse status) {
    PrintStream out = out();
    out.println("========================================");
    out.println("          Lifecycle Service Status");
    out.println("========================================");
    out.printf("IsEnabled: %s%n", status.getIsEnabled());
    if (status.getIsEnabled() && status.hasIsSuspended()) {
      out.printf("IsSuspended: %s%n", status.getIsSuspended());
    }

    if (status.getRunningBucketsCount() > 0) {
      out.println("Running Buckets:");
      for (String bucket : status.getRunningBucketsList()) {
        out.printf("  - %s%n", bucket);
      }
    } else {
      out.println("No buckets are currently being processed.");
    }
    out.println("========================================");
  }

  protected PrintStream out() {
    return System.out;
  }
}

