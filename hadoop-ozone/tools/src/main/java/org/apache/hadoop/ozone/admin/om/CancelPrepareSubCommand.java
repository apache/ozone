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

package org.apache.hadoop.ozone.admin.om;

import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

/**
 * Handler of ozone admin om cancelprepare command.
 */
@CommandLine.Command(
    name = "cancelprepare",
    description = "Cancel prepare state in the OMs.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class CancelPrepareSubCommand implements Callable<Void> {
  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(
      names = {"-id", "--service-id"},
      description = "Ozone Manager Service ID",
      required = true
  )
  private String omServiceId;

  @Override
  public Void call() throws Exception {
    OzoneManagerProtocol client = parent.createOmClient(omServiceId);
    client.cancelOzoneManagerPrepare();
    System.out.println("Cancel prepare succeeded, cluster can now accept " +
        "write requests.");
    return null;
  }
}
