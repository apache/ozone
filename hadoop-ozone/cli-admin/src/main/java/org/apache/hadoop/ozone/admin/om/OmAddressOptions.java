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
import java.util.Objects;
import org.apache.hadoop.hdds.cli.AbstractMixin;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

/** Defines command-line options for OM address, whether service or single host. */
public final class OmAddressOptions {

  /** Base class for service ID mixins. */
  protected abstract static class AbstractServiceIdMixin extends AbstractMixin {
    protected abstract ServiceIdOptions addressOptions();

    public String getServiceID() {
      ServiceIdOptions opts = addressOptions();
      return opts != null ? opts.getServiceID() : null;
    }

    public OzoneManagerProtocol newClient() throws IOException {
      return createOmClient(
          getOzoneConf(),
          rootCommand().getUser(),
          getServiceID(),
          null,
          true);
    }

    @Override
    public String toString() {
      return Objects.toString(addressOptions(), "");
    }
  }

  /** Adds host to mixin. */
  protected abstract static class AbstractServiceIdOrHostMixin extends AbstractServiceIdMixin {
    @Override
    protected abstract ServiceIdAndHostOptions addressOptions();

    public String getHost() {
      ServiceIdAndHostOptions opts = addressOptions();
      return opts != null ? opts.getHost() : null;
    }

    @Override
    public OzoneManagerProtocol newClient() throws IOException {
      return createOmClient(
          getOzoneConf(),
          rootCommand().getUser(),
          getServiceID(),
          getHost(),
          false);
    }
  }

  /** Optionally specify OM service ID. */
  public static class OptionalServiceIdMixin extends AbstractServiceIdMixin {
    @CommandLine.ArgGroup // exclusive=true, multiplicity=0..1
    private ServiceIdOptions opts;

    @Override
    protected ServiceIdOptions addressOptions() {
      return opts;
    }
  }

  /** Require OM service ID. */
  public static class MandatoryServiceIdMixin extends AbstractServiceIdMixin {
    @CommandLine.ArgGroup(multiplicity = "1") // exclusive=true
    private ServiceIdOptions opts;

    @Override
    protected ServiceIdOptions addressOptions() {
      return opts;
    }
  }

  /** Optionally specify OM service ID or host. */
  public static class OptionalServiceIdOrHostMixin extends AbstractServiceIdOrHostMixin {
    @CommandLine.ArgGroup // exclusive=true, multiplicity=0..1
    private ServiceIdAndHostOptions opts;

    @Override
    protected ServiceIdAndHostOptions addressOptions() {
      return opts;
    }
  }

  /** Require OM service ID or host. */
  public static class MandatoryServiceIdOrHostMixin extends AbstractServiceIdOrHostMixin {
    @CommandLine.ArgGroup(multiplicity = "1") // exclusive=true
    private ServiceIdAndHostOptions opts;

    @Override
    protected ServiceIdAndHostOptions addressOptions() {
      return opts;
    }
  }

  /** Options for OM service ID. */
  protected static class ServiceIdOptions {
    @CommandLine.Option(
        names = {"--service-id", "--om-service-id"},
        description = "Ozone Manager Service ID.",
        required = true
    )
    private String serviceID;

    /** For backward compatibility. */
    @CommandLine.Option(
        names = {"-id"},
        hidden = true,
        required = true
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

    @Override
    public String toString() {
      String value = getServiceID();
      return value != null && !value.isEmpty() ? "--om-service-id " + value : "";
    }
  }

  /** Add options for OM host. */
  protected static class ServiceIdAndHostOptions extends ServiceIdOptions {
    @CommandLine.Option(
        names = {"--service-host"},
        description = "Ozone Manager Host.",
        required = true
    )
    private String host;

    /** For backward compatibility. */
    @CommandLine.Option(
        names = {"-host"},
        hidden = true,
        required = true
    )
    @Deprecated
    @SuppressWarnings("DeprecatedIsStillUsed")
    private String deprecatedHost;

    public String getHost() {
      return host != null ? host : deprecatedHost;
    }

    @Override
    public String toString() {
      final String serviceOpt = super.toString();
      final String hostValue = getHost();
      return (hostValue != null && !hostValue.isEmpty())
          ? serviceOpt + " --service-host " + hostValue
          : serviceOpt;
    }
  }

  private OmAddressOptions() {
    // no instances
  }
}
