/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3secret;

/**
 * This class contains constants for configuration keys used
 * in S3 secret endpoint.
 */
public final class OzoneS3SecretConfigKeys {
  public static final String OZONE_S3G_SECRET_HTTP_ENABLED_KEY =
      "ozone.s3g.secret.http.enabled";
  public static final String OZONE_S3G_SECRET_HTTP_BIND_HOST_KEY =
      "ozone.s3g.secret.http-bind-host";
  public static final String OZONE_S3G_SECRET_HTTPS_BIND_HOST_KEY =
      "ozone.s3g.secret.https-bind-host";
  public static final String OZONE_S3G_SECRET_HTTP_ADDRESS_KEY =
      "ozone.s3g.secret.http-address";
  public static final String OZONE_S3G_SECRET_HTTPS_ADDRESS_KEY =
      "ozone.s3g.secret.https-address";

  public static final String OZONE_S3G_SECRET_HTTP_BIND_HOST_DEFAULT =
      "0.0.0.0";
  public static final int OZONE_S3G_SECRET_HTTP_BIND_PORT_DEFAULT = 9978;
  public static final int OZONE_S3G_SECRET_HTTPS_BIND_PORT_DEFAULT = 9979;

  public static final String OZONE_S3G_SECRET_HTTP_AUTH_CONFIG_PREFIX =
      "ozone.s3g.secret.http.auth.";
  public static final String OZONE_S3G_SECRET_HTTP_AUTH_TYPE =
      OZONE_S3G_SECRET_HTTP_AUTH_CONFIG_PREFIX + "type";
  public static final String OZONE_S3G_SECRET_KEYTAB_FILE =
      OZONE_S3G_SECRET_HTTP_AUTH_CONFIG_PREFIX + "kerberos.keytab";
  public static final String
      OZONE_S3G_SECRET_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL =
      OZONE_S3G_SECRET_HTTP_AUTH_CONFIG_PREFIX + "kerberos.principal";

  /**
   * Never constructed.
   */
  private OzoneS3SecretConfigKeys() {

  }
}
