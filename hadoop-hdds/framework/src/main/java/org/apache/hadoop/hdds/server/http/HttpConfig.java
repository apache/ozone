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

package org.apache.hadoop.hdds.server.http;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.ozone.OzoneConfigKeys;

/**
 * Singleton to get access to Http related configuration.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class HttpConfig {

  private HttpConfig() {
  }

  /**
   * Enum for different kind of security combinations.
   */
  public enum Policy {
    HTTP_ONLY,
    HTTPS_ONLY,
    HTTP_AND_HTTPS;

    private static final Policy[] VALUES = values();

    public static Policy fromString(String value) {
      for (Policy p : VALUES) {
        if (p.name().equalsIgnoreCase(value)) {
          return p;
        }
      }
      return null;
    }

    public boolean isHttpEnabled() {
      return this == HTTP_ONLY || this == HTTP_AND_HTTPS;
    }

    public boolean isHttpsEnabled() {
      return this == HTTPS_ONLY || this == HTTP_AND_HTTPS;
    }
  }

  public static Policy getHttpPolicy(MutableConfigurationSource conf) {
    String policyStr = conf.get(OzoneConfigKeys.OZONE_HTTP_POLICY_KEY,
        OzoneConfigKeys.OZONE_HTTP_POLICY_DEFAULT);
    HttpConfig.Policy policy = HttpConfig.Policy.fromString(policyStr);
    if (policy == null) {
      throw new IllegalArgumentException("Unrecognized value '"
          + policyStr + "' for " + OzoneConfigKeys.OZONE_HTTP_POLICY_KEY);
    }
    conf.set(OzoneConfigKeys.OZONE_HTTP_POLICY_KEY, policy.name());
    return policy;
  }
}
