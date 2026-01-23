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

import org.apache.hadoop.hdds.cli.AdminSubcommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

/**
 * Subcommand for admin operations related to SCM.
 */
@CommandLine.Command(
    name = "scm",
    description = "Ozone Storage Container Manager specific admin operations",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        GetScmRatisRolesSubcommand.class,
        FinalizeScmUpgradeSubcommand.class,
        FinalizationScmStatusSubcommand.class,
        TransferScmLeaderSubCommand.class,
        DecommissionScmSubcommand.class,
        RotateKeySubCommand.class,
        DeletedBlocksTxnCommands.class
    })
@MetaInfServices(AdminSubcommand.class)
public class ScmAdmin implements AdminSubcommand {

  @CommandLine.ParentCommand
  private OzoneAdmin parent;

  public OzoneAdmin getParent() {
    return parent;
  }
}
