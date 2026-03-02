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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmSecurityClient;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.cli.AbstractMixin;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB.ScmNodeTarget;
import picocli.CommandLine;

/**
 * Defines command-line option for SCM address.
 */
public class ScmOption extends AbstractMixin {

  @CommandLine.Option(names = {"--scm"},
      description = "The destination scm (host:port)")
  private String scm;

  @CommandLine.Option(names = {"--service-id", "-id"}, description =
      "ServiceId of SCM HA Cluster")
  private String scmServiceId;

  public ScmClient createScmClient() throws IOException {
    OzoneConfiguration conf = getOzoneConf();
    checkAndSetSCMAddressArg(conf);

    return new ContainerOperationClient(conf);
  }

  public ScmClient createScmClient(OzoneConfiguration conf) throws IOException {
    checkAndSetSCMAddressArg(conf);
    return new ContainerOperationClient(conf);
  }

  public ScmClient createScmClient(OzoneConfiguration conf, ScmNodeTarget targetScmNode) throws IOException {
    checkAndSetSCMAddressArg(conf);
    return new ContainerOperationClient(conf, targetScmNode);
  }

  private void checkAndSetSCMAddressArg(MutableConfigurationSource conf) {
    if (StringUtils.isNotEmpty(scm)) {
      conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, scm);
    }

    // Use the scm service Id passed from the client.

    if (StringUtils.isNotEmpty(scmServiceId)) {
      conf.set(ScmConfigKeys.OZONE_SCM_DEFAULT_SERVICE_ID, scmServiceId);
    } else if (StringUtils.isBlank(HddsUtils.getScmServiceId(conf))) {
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
      return getScmSecurityClient(getOzoneConf());
    } catch (IOException ex) {
      throw new IllegalArgumentException(
          "Can't create SCM Security client", ex);
    }
  }

  public String getScm() {
    return scm;
  }
}
