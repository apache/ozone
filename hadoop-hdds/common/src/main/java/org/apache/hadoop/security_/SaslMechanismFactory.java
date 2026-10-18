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
  private static volatile String mechanism;

  private static synchronized String getSynchronously() {
    // env
    final String envValue = System.getenv(SASL_MECHANISM_ENV);
    LOG.debug("{} = {} (env)", SASL_MECHANISM_ENV, envValue);

    // conf
    final Configuration conf = new Configuration();
    final String confValue = conf.get(HADOOP_SECURITY_SASL_MECHANISM_KEY,
        HADOOP_SECURITY_SASL_MECHANISM_DEFAULT);
    LOG.debug("{} = {} (conf)", HADOOP_SECURITY_SASL_MECHANISM_KEY, confValue);

    mechanism = envValue != null ? envValue
        : confValue != null ? confValue
        : HADOOP_SECURITY_SASL_MECHANISM_DEFAULT;
    LOG.debug("SASL_MECHANISM = {} (effective)", mechanism);
    return mechanism;
  }

  public static String getMechanism() {
    final String value = mechanism;
    return value != null ? value : getSynchronously();
  }

  public static boolean isDefaultMechanism(AuthMethod authMethod) {
    return HADOOP_SECURITY_SASL_MECHANISM_DEFAULT.equals(getMechanismName(authMethod));
  }

  public static boolean isDigestMechanism(AuthMethod authMethod) {
    return getMechanismName(authMethod).startsWith("DIGEST-");
  }

  private SaslMechanismFactory() {}

  public static void main(String[] args) {
    System.out.println("SASL_MECHANISM = " + getMechanism());
  }

  /** Helper to get actual mechanism name from config.  Required because {@code AuthMethod} is from Hadoop,
   * not forked (because it is used in UGI, etc.). */
  public static String getMechanismName(AuthMethod authMethod) {
    switch (authMethod) {
    case DIGEST:
    case TOKEN:
      return getMechanism();
    default:
      return authMethod.getMechanismName();

    }
  }
}
