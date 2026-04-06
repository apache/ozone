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

/**
 * Validates Hadoop and Ozone security configuration.
 *
 * This probe verifies that Kerberos authentication is enabled
 * and prints related security configuration values used by
 * Ozone services.
 */
public class SecurityConfigProbe extends ConfigProbe {

  @Override
  public String name() {
    return "Security Configuration";
  }

  @Override
  public boolean test(OzoneConfiguration conf) {

    // Print all relevant configs
    print(conf, CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION);
    print(conf, OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY);
    print(conf, OzoneConfigKeys.OZONE_HTTP_SECURITY_ENABLED_KEY);
    print(conf, CommonConfigurationKeysPublic.HADOOP_RPC_PROTECTION);
    print(conf, CommonConfigurationKeysPublic.HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS);
    print(conf, OzoneConfigKeys.OZONE_ADMINISTRATORS);
    print(conf, OzoneConfigKeys.OZONE_S3_ADMINISTRATORS);
    print(conf, HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED);
    print(conf, HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED);
    print(conf, HddsConfigKeys.HDDS_GRPC_TLS_ENABLED);

    String auth = conf.getTrimmed(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION);
    String ozoneSecurity = conf.getTrimmed(
        OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY);

    boolean valid = true;
    if (!"kerberos".equalsIgnoreCase(auth)) {
      warn("Kerberos is not enabled (current: " + auth + ")");
      valid = false;
    }

    if (!Boolean.parseBoolean(ozoneSecurity)) {
      warn("Ozone security is not enabled (current: " + ozoneSecurity + ")");
      valid = false;
    }

    return valid;
  }
}
