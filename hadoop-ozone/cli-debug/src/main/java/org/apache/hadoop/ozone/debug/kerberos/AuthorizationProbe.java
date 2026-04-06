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

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMConfigKeys;

/**
 * Validates Ozone and Hadoop RPC authorization configuration.
 * Checks whether Hadoop authorization and Ozone ACL settings are enabled
 * and properly configured.
 * Missing or disabled authorization is reported as a warning.
 */
public class AuthorizationProbe extends ConfigProbe {

  @Override
  public String name() {
    return "Authorization Configuration";
  }

  @Override
  public boolean test(OzoneConfiguration conf) {

    boolean valid = true;

    // Print relevant configs
    print(conf, OzoneConfigKeys.OZONE_ACL_ENABLED);
    print(conf, OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS);
    print(conf, CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION);
    print(conf, OMConfigKeys.OZONE_OM_SECURITY_CLIENT_PROTOCOL_ACL);

    print(conf, HddsConfigKeys.HDDS_SECURITY_CLIENT_DATANODE_CONTAINER_PROTOCOL_ACL);
    print(conf, HddsConfigKeys.HDDS_SECURITY_CLIENT_SCM_CONTAINER_PROTOCOL_ACL);
    print(conf, HddsConfigKeys.HDDS_SECURITY_CLIENT_SCM_BLOCK_PROTOCOL_ACL);
    print(conf, HddsConfigKeys.HDDS_SECURITY_CLIENT_SCM_CERTIFICATE_PROTOCOL_ACL);

    // Validate Hadoop authorization
    boolean hadoopAuth = Boolean.parseBoolean(
        conf.getTrimmed(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION));

    if (!hadoopAuth) {
      warn("Hadoop authorization is disabled");
      valid = false;
    }

    // Validate Ozone ACLs
    boolean ozoneAcl = Boolean.parseBoolean(
        conf.getTrimmed(OzoneConfigKeys.OZONE_ACL_ENABLED));

    if (!ozoneAcl) {
      warn("Ozone ACLs are disabled");
      valid = false;
    }

    return valid;
  }
}
