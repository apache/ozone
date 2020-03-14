/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.admin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.cli.ReplicationManagerCommands;
import org.apache.hadoop.hdds.scm.cli.SafeModeCommands;
import org.apache.hadoop.hdds.scm.cli.TopologySubcommand;
import org.apache.hadoop.hdds.scm.cli.container.ContainerCommands;
import org.apache.hadoop.hdds.scm.cli.container.WithScmClient;
import org.apache.hadoop.hdds.scm.cli.datanode.DatanodeCommands;
import org.apache.hadoop.hdds.scm.cli.pipeline.PipelineCommands;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.ozone.admin.om.OMAdmin;
import org.apache.hadoop.util.NativeCodeLoader;

import org.apache.commons.lang3.StringUtils;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * Ozone Admin Command line tool.
 */
@CommandLine.Command(name = "ozone admin",
    hidden = true,
    description = "Developer tools for Ozone Admin operations",
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        OMAdmin.class,
        SafeModeCommands.class,
        ContainerCommands.class,
        PipelineCommands.class,
        DatanodeCommands.class,
        TopologySubcommand.class,
        ReplicationManagerCommands.class
    },
    mixinStandardHelpOptions = true)
public class OzoneAdmin extends GenericCli implements WithScmClient {

  private OzoneConfiguration ozoneConf;

  @Option(names = {"--scm"}, description = "The destination scm (host:port)")
  private String scm = "";

  public OzoneConfiguration getOzoneConf() {
    if (ozoneConf == null) {
      ozoneConf = createOzoneConfiguration();
    }
    return ozoneConf;
  }

  /**
   * Main for the Ozone Admin shell Command handling.
   *
   * @param argv - System Args Strings[]
   * @throws Exception
   */
  public static void main(String[] argv) throws Exception {
    LogManager.resetConfiguration();
    Logger.getRootLogger().setLevel(Level.INFO);
    Logger.getRootLogger()
        .addAppender(new ConsoleAppender(new PatternLayout("%m%n")));
    Logger.getLogger(NativeCodeLoader.class).setLevel(Level.ERROR);

    new OzoneAdmin().run(argv);
  }

  public ScmClient createScmClient() {
    try {
      OzoneConfiguration conf = createOzoneConfiguration();
      checkAndSetSCMAddressArg(conf);

      return new ContainerOperationClient(conf);
    } catch (IOException ex) {
      throw new IllegalArgumentException("Can't create SCM client", ex);
    }
  }

  private void checkAndSetSCMAddressArg(Configuration conf) {
    if (StringUtils.isNotEmpty(scm)) {
      conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, scm);
    }
    if (!HddsUtils.getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY).isPresent()) {

      throw new IllegalArgumentException(
          ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY
              + " should be set in ozone-site.xml or with the --scm option");
    }
  }
}
