/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.cli.container;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hdds.scm.cli.OzoneCommandHandler;
import org.apache.hadoop.hdds.scm.cli.SCMCLI;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;

import java.io.IOException;

/**
 * The handler of close container command.
 */
public class CloseContainerHandler extends OzoneCommandHandler {

  public static final String CONTAINER_CLOSE = "close";
  public static final String OPT_CONTAINER_NAME = "c";

  @Override
  public void execute(CommandLine cmd) throws IOException {
    if (!cmd.hasOption(CONTAINER_CLOSE)) {
      throw new IOException("Expecting container close");
    }
    if (!cmd.hasOption(OPT_CONTAINER_NAME)) {
      displayHelp();
      if (!cmd.hasOption(SCMCLI.HELP_OP)) {
        throw new IOException("Expecting container name");
      } else {
        return;
      }
    }
    String containerName = cmd.getOptionValue(OPT_CONTAINER_NAME);

    Pipeline pipeline = getScmClient().getContainer(containerName);
    if (pipeline == null) {
      throw new IOException("Cannot close an non-exist container "
          + containerName);
    }
    logOut("Closing container : %s.", containerName);
    getScmClient().closeContainer(pipeline);
    logOut("Container closed.");
  }

  @Override
  public void displayHelp() {
    Options options = new Options();
    addOptions(options);
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter
        .printHelp(SCMCLI.CMD_WIDTH, "hdfs scm -container -close <option>",
            "where <option> is", options, "");
  }

  public static void addOptions(Options options) {
    Option containerNameOpt = new Option(OPT_CONTAINER_NAME,
        true, "Specify container name");
    options.addOption(containerNameOpt);
  }

  CloseContainerHandler(ScmClient client) {
    super(client);
  }
}
