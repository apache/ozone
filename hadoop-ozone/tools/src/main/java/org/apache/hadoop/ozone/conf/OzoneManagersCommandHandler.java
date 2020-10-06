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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.conf;

import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/**
 * Handler for ozone getconf ozonemanagers.
 */
@Command(name = "ozonemanagers",
    aliases = {"-ozonemanagers"},
    description = "gets list of ozone storage container "
        + "manager nodes in the cluster",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class OzoneManagersCommandHandler implements Callable<Void> {

  @ParentCommand
  private OzoneGetConf tool;

  @Override
  public Void call() throws Exception {
    ConfigurationSource configSource =
        OzoneConfiguration.of(tool.getConf());
    if (OmUtils.isServiceIdsDefined(
        configSource)) {
      tool.printOut(OmUtils.getOmHAAddressesById(configSource).toString());
    } else {
      tool.printOut(OmUtils.getOmAddress(configSource).getHostName());
    }
    return null;
  }
}
