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

package org.apache.hadoop.ozone.debug.kerberos;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;

/**
 * Prints HTTP Kerberos authentication configuration for Ozone services.
 * This probe checks whether the HTTP endpoints (WebUI / REST services)
 * of Ozone components are configured to use Kerberos authentication.
 * It only prints configuration values for diagnostics and does not
 * enforce validation.
 */
public class HttpAuthProbe extends ConfigProbe {

  @Override
  public String name() {
    return "HTTP Kerberos Authentication";
  }

  @Override
  public ProbeResult test(OzoneConfiguration conf) {

    print(conf, OMConfigKeys.OZONE_OM_HTTP_AUTH_TYPE);
    print(conf, HddsConfigKeys.HDDS_SCM_HTTP_AUTH_TYPE);
    print(conf, HddsConfigKeys.HDDS_DATANODE_HTTP_AUTH_TYPE);
    print(conf, ReconServerConfigKeys.OZONE_RECON_HTTP_AUTH_TYPE);
    //Used key directly to avoid cyclic dependency
    print(conf, "ozone.s3g.http.auth.type");

    return ProbeResult.PASS;
  }
}
