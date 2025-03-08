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

package org.apache.hadoop.ozone.utils;

import org.apache.hadoop.hdds.scm.server.StorageContainerManagerStarter;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.apache.hadoop.ozone.audit.parser.AuditParser;
import org.apache.hadoop.ozone.conf.OzoneGetConf;
import org.apache.hadoop.ozone.csi.CsiServer;
import org.apache.hadoop.ozone.debug.OzoneDebug;
import org.apache.hadoop.ozone.freon.Freon;
import org.apache.hadoop.ozone.genconf.GenerateOzoneRequiredConfigurations;
import org.apache.hadoop.ozone.insight.Insight;
import org.apache.hadoop.ozone.om.OzoneManagerStarter;
import org.apache.hadoop.ozone.recon.ReconServer;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.apache.hadoop.ozone.s3.Gateway;
import org.apache.hadoop.ozone.shell.OzoneRatis;
import org.apache.hadoop.ozone.shell.OzoneShell;
import org.apache.hadoop.ozone.shell.checknative.CheckNative;
import org.apache.hadoop.ozone.shell.s3.S3Shell;
import org.apache.hadoop.ozone.shell.tenant.TenantShell;
import org.apache.hadoop.util.Classpath;

import picocli.AutoComplete;
import picocli.CommandLine;

public class AutoCompletion {

  public static void main(String[] args) {

    CommandLine hierarchy = new CommandLine(new Ozone())
        .addSubcommand("admin", new OzoneAdmin().getCmd())
        .addSubcommand("auditparser", new AuditParser().getCmd())
        .addSubcommand("checknative", new CheckNative().getCmd())
        .addSubcommand("classpath", new Classpath())
        .addSubcommand("csi", new CsiServer().getCmd())
        .addSubcommand("datanode", new HddsDatanodeService().getCmd())
        .addSubcommand("debug", new OzoneDebug().getCmd())
        .addSubcommand("freon", new Freon().getCmd())
        .addSubcommand("genconf", new GenerateOzoneRequiredConfigurations().getCmd())
        .addSubcommand("getconf", new OzoneGetConf().getCmd())
        .addSubcommand("insight", new Insight().getCmd())
        .addSubcommand("om", new OzoneManagerStarter(null).getCmd())
        .addSubcommand("ratis", new OzoneRatis().getCmd())
        .addSubcommand("recon", new ReconServer().getCmd())
        .addSubcommand("repair", new OzoneRepair().getCmd())
        .addSubcommand("s3", new S3Shell().getCmd())
        .addSubcommand("s3g", new Gateway().getCmd())
        .addSubcommand("scm", new StorageContainerManagerStarter(null).getCmd())
        .addSubcommand("sh", new OzoneShell().getCmd(), "shell")
        .addSubcommand("tenant", new TenantShell().getCmd());

    /*
     * The following commands are marked as hidden,
     * so they won't have completed.
     *   - OzoneAdmin
     *   - Insight
     *   - CsiServer
     *   - HddsDatanodeService
     *   - OzoneManagerStarter
     *   - Gateway
     *   - StorageContainerManagerStarter
     */

    /*
     * We don't have auto-complete support for the following commands 
     *   - dtutil
     *   - envvars
     *   - fs
     *   - insight
     *   - daemonlog
     */

    System.out.println(AutoComplete.bash("ozone",hierarchy));
  }

  /**
   * Ozone top level command, used only to generate auto-complete.
   */
  @CommandLine.Command(name = "ozone",
          description = "Ozone top level command")
  private static class Ozone {
  }
}
