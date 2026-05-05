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

package org.apache.hadoop.ozone.recon;

import com.google.inject.Inject;
import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfigKeys;
import org.apache.hadoop.hdds.server.http.BaseHttpServer;

/**
 * Recon http server with recon supplied config defaults.
 */

public class ReconHttpServer extends BaseHttpServer {

  @Inject
  ReconHttpServer(OzoneConfiguration conf) throws IOException {
    super(conf, "recon");
  }

  @Override
  protected String getHttpAddressKey() {
    return ReconConfigKeys.OZONE_RECON_HTTP_ADDRESS_KEY;
  }

  @Override
  protected String getHttpsAddressKey() {
    return ReconConfigKeys.OZONE_RECON_HTTPS_ADDRESS_KEY;
  }

  @Override
  protected String getHttpBindHostKey() {
    return ReconServerConfigKeys.OZONE_RECON_HTTP_BIND_HOST_KEY;
  }

  @Override
  protected String getHttpsBindHostKey() {
    return ReconServerConfigKeys.OZONE_RECON_HTTPS_BIND_HOST_KEY;
  }

  @Override
  protected String getBindHostDefault() {
    return ReconServerConfigKeys.OZONE_RECON_HTTP_BIND_HOST_DEFAULT;
  }

  @Override
  protected int getHttpBindPortDefault() {
    return ReconServerConfigKeys.OZONE_RECON_HTTP_BIND_PORT_DEFAULT;
  }

  @Override
  protected int getHttpsBindPortDefault() {
    return ReconServerConfigKeys.OZONE_RECON_HTTPS_BIND_PORT_DEFAULT;
  }

  @Override
  protected String getKeytabFile() {
    return ReconServerConfigKeys.OZONE_RECON_HTTP_KEYTAB_FILE;
  }

  @Override
  protected String getSpnegoPrincipal() {
    return ReconServerConfigKeys
        .OZONE_RECON_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL;
  }

  @Override
  protected String getEnabledKey() {
    return ReconServerConfigKeys.OZONE_RECON_HTTP_ENABLED_KEY;
  }

  @Override
  protected String getHttpAuthType() {
    return ReconServerConfigKeys.OZONE_RECON_HTTP_AUTH_TYPE;
  }

  @Override
  protected String getHttpAuthConfigPrefix() {
    return ReconServerConfigKeys.OZONE_RECON_HTTP_AUTH_CONFIG_PREFIX;
  }
}
