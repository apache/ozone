/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security_;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SASL related constants.
 */
public final class SaslMechanismFactory {
  static final Logger LOG = LoggerFactory.getLogger(SaslMechanismFactory.class);

  public static final String HADOOP_SECURITY_SASL_MECHANISM_KEY
      = "hadoop.security.sasl.mechanism";
  public static final String HADOOP_SECURITY_SASL_MECHANISM_DEFAULT
      = "DIGEST-MD5";
  public static final String HADOOP_SECURITY_SASL_CUSTOMIZEDCALLBACKHANDLER_CLASS_KEY
      = "hadoop.security.sasl.CustomizedCallbackHandler.class";

  private static final String SASL_MECHANISM_ENV = "HADOOP_SASL_MECHANISM";
  private static final String SASL_MECHANISM;

  static {
    // env
    final String envValue = System.getenv(SASL_MECHANISM_ENV);
    LOG.debug("{} = {} (env)", SASL_MECHANISM_ENV, envValue);

    // conf
    final Configuration conf = new Configuration(false);
    final String confValue = conf.get(HADOOP_SECURITY_SASL_MECHANISM_KEY,
        HADOOP_SECURITY_SASL_MECHANISM_DEFAULT);
    LOG.debug("{} = {} (conf)", HADOOP_SECURITY_SASL_MECHANISM_KEY, confValue);

    if (envValue != null && confValue != null) {
      if (!envValue.equals(confValue)) {
        throw new IllegalArgumentException("SASL Mechanism mismatched: env "
            + SASL_MECHANISM_ENV + " is " + envValue + " but conf "
            + HADOOP_SECURITY_SASL_MECHANISM_KEY + " is " + confValue);
      }
    }

    SASL_MECHANISM = envValue != null ? envValue
        : confValue != null ? confValue
        : HADOOP_SECURITY_SASL_MECHANISM_DEFAULT;
    LOG.debug("SASL_MECHANISM = {} (effective)", SASL_MECHANISM);
  }

  public static String getMechanism() {
    return SASL_MECHANISM;
  }

  public static boolean isDefaultMechanism(AuthMethod authMethod) {
    String mechanism = SaslRpcClient.getMechanismName(authMethod);
    return HADOOP_SECURITY_SASL_MECHANISM_DEFAULT.equals(mechanism);
  }

  private SaslMechanismFactory() {}
}
