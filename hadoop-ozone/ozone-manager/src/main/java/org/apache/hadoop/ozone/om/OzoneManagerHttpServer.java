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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_HTTP_ENDPOINT_V2;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_SERVICE_LIST_HTTP_ENDPOINT;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.server.http.BaseHttpServer;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * HttpServer wrapper for the OzoneManager.
 */
public class OzoneManagerHttpServer extends BaseHttpServer {

  public OzoneManagerHttpServer(MutableConfigurationSource conf,
      OzoneManager om) throws IOException {
    super(conf, "ozoneManager");
    addServlet("serviceList", OZONE_OM_SERVICE_LIST_HTTP_ENDPOINT,
        ServiceListJSONServlet.class);
    addServlet("dbCheckpoint", OZONE_DB_CHECKPOINT_HTTP_ENDPOINT,
        OMDBCheckpointServlet.class);
    addServlet("dbCheckpointv2", OZONE_DB_CHECKPOINT_HTTP_ENDPOINT_V2,
        OMDBCheckpointServletInodeBasedXfer.class);
    getWebAppContext().setAttribute(OzoneConsts.OM_CONTEXT_ATTRIBUTE, om);
  }

  @Override protected String getHttpAddressKey() {
    return OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY;
  }

  @Override protected String getHttpBindHostKey() {
    return OMConfigKeys.OZONE_OM_HTTP_BIND_HOST_KEY;
  }

  @Override protected String getHttpsAddressKey() {
    return OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY;
  }

  @Override protected String getHttpsBindHostKey() {
    return OMConfigKeys.OZONE_OM_HTTPS_BIND_HOST_KEY;
  }

  @Override protected String getBindHostDefault() {
    return OMConfigKeys.OZONE_OM_HTTP_BIND_HOST_DEFAULT;
  }

  @Override protected int getHttpBindPortDefault() {
    return OMConfigKeys.OZONE_OM_HTTP_BIND_PORT_DEFAULT;
  }

  @Override protected int getHttpsBindPortDefault() {
    return OMConfigKeys.OZONE_OM_HTTPS_BIND_PORT_DEFAULT;
  }

  @Override protected String getKeytabFile() {
    return OMConfigKeys.OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE;
  }

  @Override protected String getSpnegoPrincipal() {
    return OMConfigKeys.OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY;
  }

  @Override protected String getEnabledKey() {
    return OMConfigKeys.OZONE_OM_HTTP_ENABLED_KEY;
  }

  @Override
  protected String getHttpAuthType() {
    return OMConfigKeys.OZONE_OM_HTTP_AUTH_TYPE;
  }

  @Override
  protected String getHttpAuthConfigPrefix() {
    return OMConfigKeys.OZONE_OM_HTTP_AUTH_CONFIG_PREFIX;
  }
}
