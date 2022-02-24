/*
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
package org.apache.hadoop.hdds.scm.cli;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.cli.GenericParentCommand;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import picocli.CommandLine;

import java.io.IOException;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmSecurityClient;
import static picocli.CommandLine.Spec.Target.MIXEE;

/**
 * Defines command-line option for SCM address.
 */
public class ScmOption {

  @CommandLine.Spec(MIXEE)
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(names = {"--scm"},
      description = "The destination scm (host:port)")
  private String scm;


  @CommandLine.Option(names = {"--service-id", "-id"}, description =
      "ServiceId of SCM HA Cluster")
  private String scmServiceId;

  public ScmClient createScmClient() {
    GenericParentCommand parent = (GenericParentCommand)
        spec.root().userObject();
    OzoneConfiguration conf = parent.createOzoneConfiguration();
    checkAndSetSCMAddressArg(conf);

    return new ContainerOperationClient(conf);
  }

  private void checkAndSetSCMAddressArg(MutableConfigurationSource conf) {
    if (StringUtils.isNotEmpty(scm)) {
      conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, scm);
    }

    // Use the scm service Id passed from the client.

    if (StringUtils.isNotEmpty(scmServiceId)) {
      conf.set(ScmConfigKeys.OZONE_SCM_DEFAULT_SERVICE_ID, scmServiceId);
    } else if (StringUtils.isBlank(SCMHAUtils.getScmServiceId(conf))) {
      // Scm service id is not passed, and scm service id is not defined in
      // the config, assuming it should be non-HA cluster.
      if (!HddsUtils.getHostNameFromConfigKeys(conf,
          ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY).isPresent()) {

        throw new ConfigurationException(
            ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY
                + " should be set in ozone-site.xml or with the --scm option");
      }
    }

  }

  public SCMSecurityProtocol createScmSecurityClient() {
    try {
      GenericParentCommand parent = (GenericParentCommand)
          spec.root().userObject();
      return getScmSecurityClient(parent.createOzoneConfiguration());
    } catch (IOException ex) {
      throw new IllegalArgumentException(
          "Can't create SCM Security client", ex);
    }
  }

}
