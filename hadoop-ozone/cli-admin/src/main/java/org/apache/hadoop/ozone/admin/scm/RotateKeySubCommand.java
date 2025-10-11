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

package org.apache.hadoop.ozone.admin.scm;

import java.io.IOException;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;

/**
 * Handler of ozone admin scm rotate command.
 */
@CommandLine.Command(
    name = "rotate",
    description = "CLI command to force generate new keys (rotate)",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class RotateKeySubCommand extends ScmSubcommand {

  @CommandLine.Option(names = "--force",
      description = "Force generate new keys")
  private boolean force = false;

  @CommandLine.ParentCommand
  private ScmAdmin parent;

  @Override
  protected void execute(ScmClient scmClient) throws IOException {
    try (ScmClient client = new ContainerOperationClient(
        parent.getParent().getOzoneConf())) {
      boolean status = false;
      try {
        status = client.rotateSecretKeys(force);
      } catch (IOException e) {
        System.err.println("Secret key rotation failed: " + e.getMessage());
        return;
      }
      if (status) {
        System.out.println("Secret key rotation is complete. A new key has " +
            "been generated.");
      }
    }
  }
}

