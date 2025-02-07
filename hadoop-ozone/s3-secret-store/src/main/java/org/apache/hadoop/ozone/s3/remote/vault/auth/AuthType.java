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

package org.apache.hadoop.ozone.s3.remote.vault.auth;

import static org.apache.hadoop.ozone.s3.remote.S3SecretRemoteStoreConfigurationKeys.APP_ROLE_ID;
import static org.apache.hadoop.ozone.s3.remote.S3SecretRemoteStoreConfigurationKeys.APP_ROLE_PATH;
import static org.apache.hadoop.ozone.s3.remote.S3SecretRemoteStoreConfigurationKeys.APP_ROLE_SECRET;
import static org.apache.hadoop.ozone.s3.remote.S3SecretRemoteStoreConfigurationKeys.AUTH_TYPE;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.s3.remote.S3SecretRemoteStoreConfigurationKeys;

/**
 * Type of authentication method.
 */
public enum AuthType {
  APP_ROLE,
  TOKEN;

  public static Auth fromConf(Configuration conf) {
    AuthType authType = AuthType.valueOf(conf.get(AUTH_TYPE).toUpperCase());
    switch (authType) {
    case TOKEN:
      String token = conf.get(S3SecretRemoteStoreConfigurationKeys.TOKEN);
      return new DirectTokenAuth(() -> token);
    case APP_ROLE:
      String rolePath = conf.get(APP_ROLE_PATH);
      String roleId = conf.get(APP_ROLE_ID);
      String roleSecret = conf.get(APP_ROLE_SECRET);
      return new AppRoleAuth(rolePath, roleId, roleSecret);
    default:
      throw new IllegalStateException(
          "Vault authentication method doesn't specified.");
    }
  }
}
