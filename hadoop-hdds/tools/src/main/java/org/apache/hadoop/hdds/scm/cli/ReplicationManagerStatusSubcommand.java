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

package org.apache.hadoop.hdds.scm.cli;

import java.io.IOException;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine.Command;

/**
 * Handler to query status of replication manager.
 */
@Command(
    name = "status",
    description = "Check if ReplicationManager is running or not",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ReplicationManagerStatusSubcommand extends ScmSubcommand {

  @Override
  public void execute(ScmClient scmClient) throws IOException {
    boolean execReturn = scmClient.getReplicationManagerStatus();

    // Output data list
    if (execReturn) {
      System.out.println("ReplicationManager is Running.");
    } else {
      System.out.println("ReplicationManager is Not Running.");
    }
  }
}
