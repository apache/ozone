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

/** Defines command-line options for OM service ID. */
public class OmServiceOption extends AbstractMixin {

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
    private String deprecatedID;

    public String getServiceID() {
      if (serviceID != null) {
        return serviceID;
      }
      return deprecatedID;
    }
  }

  public String getServiceID() {
    return exclusive != null ? exclusive.getServiceID() : null;
  }

  public OzoneManagerProtocol newClient() throws IOException {
    return createOmClient(
        getOzoneConf(),
        rootCommand().getUser(),
        getServiceID(),
        null,
        true);
  }

  public static String toString(String value) {
    return value != null && !value.isEmpty() ? "--om-service-id " + value : "";
  }
}
