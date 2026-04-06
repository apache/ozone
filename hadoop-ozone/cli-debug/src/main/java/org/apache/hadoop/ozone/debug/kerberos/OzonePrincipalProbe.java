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
import org.apache.hadoop.hdds.recon.ReconConfig;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.ozone.om.OMConfigKeys;

/**
 * Prints configured service principals.
 */
public class OzonePrincipalProbe extends ConfigProbe {

  @Override
  public String name() {
    return "Ozone Service Principals";
  }

  @Override
  public boolean test(OzoneConfiguration conf) {

    print(conf, OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY);
    print(conf, ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY);
    print(conf, HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY);
    print(conf, ReconConfig.ConfigStrings.OZONE_RECON_KERBEROS_PRINCIPAL_KEY);
    //Used key directly to avoid cyclic dependency.
    print(conf, "ozone.s3g.kerberos.principal");
    return true;
  }

}
