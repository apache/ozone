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

package org.apache.hadoop.ozone.admin.om;

import static org.apache.hadoop.ozone.admin.om.OMAdmin.createOmClient;

import java.io.IOException;
import org.apache.hadoop.hdds.cli.AbstractMixin;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

/** Defines command-line options for OM address, whether service or single host. */
public class OmAddressOptions extends AbstractMixin {

  @CommandLine.ArgGroup
  private Exclusive exclusive;

  static class Exclusive {
    @CommandLine.Option(
        names = {"--service-id", "--om-service-id"},
        description = "Ozone Manager Service ID."
    )
    private String serviceID;

    @CommandLine.Option(
        names = {"-id"},
        hidden = true
    )
    @Deprecated
    @SuppressWarnings("DeprecatedIsStillUsed")
    private String id;

    @CommandLine.Option(
        names = {"--service-host"}
    )
    private String host;

    @CommandLine.Option(
        names = {"-host"},
        hidden = true
    )
    @Deprecated
    @SuppressWarnings("DeprecatedIsStillUsed")
    private String deprecatedHost;

    public String getHost() {
      return host != null ? host : deprecatedHost;
    }

    public String getServiceID() {
      if (serviceID != null) {
        return serviceID;
      }
      return id;
    }
  }

  public String getServiceID() {
    return exclusive != null ? exclusive.getServiceID() : null;
  }

  public String getHost() {
    return exclusive != null ? exclusive.getHost() : null;
  }

  public OzoneManagerProtocol newClient() throws IOException {
    return createOmClient(
        getOzoneConf(),
        rootCommand().getUser(),
        getServiceID(),
        getHost(),
        false);
  }

  @Override
  public String toString() {
    String host = getHost();
    String str = OmServiceOption.toString(getServiceID())
        + " "
        + (host != null && !host.isEmpty() ? "--service-host " + host : "");
    return str.trim();
  }

}
