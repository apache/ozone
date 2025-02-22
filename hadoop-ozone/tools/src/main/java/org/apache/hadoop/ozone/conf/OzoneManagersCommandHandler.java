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

package org.apache.hadoop.ozone.conf;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.ozone.OmUtils.getOmHAAddressesById;

import java.net.InetSocketAddress;
import java.util.Collection;
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
    description = "gets list of Ozone Manager nodes in the cluster",
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
      Collection<InetSocketAddress> omAddresses =
          getOmHAAddressesById(configSource)
              .values().stream().flatMap(Collection::stream)
              .collect(toList());
      for (InetSocketAddress addr : omAddresses) {
        tool.printOut(addr.getHostName());
      }
    } else {
      tool.printOut(OmUtils.getOmAddress(configSource).getHostName());
    }
    return null;
  }
}
