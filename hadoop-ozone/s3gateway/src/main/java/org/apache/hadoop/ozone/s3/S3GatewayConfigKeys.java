/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.s3;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;

/**
 * This class contains constants for configuration keys used in S3G.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class S3GatewayConfigKeys {

  public static final String OZONE_S3G_HTTP_ENABLED_KEY =
      "ozone.s3g.http.enabled";
  public static final String OZONE_S3G_HTTP_BIND_HOST_KEY =
      "ozone.s3g.http-bind-host";
  public static final String OZONE_S3G_HTTPS_BIND_HOST_KEY =
      "ozone.s3g.https-bind-host";
  public static final String OZONE_S3G_HTTP_ADDRESS_KEY =
      "ozone.s3g.http-address";
  public static final String OZONE_S3G_HTTPS_ADDRESS_KEY =
      "ozone.s3g.https-address";

  public static final String OZONE_S3G_HTTP_BIND_HOST_DEFAULT = "0.0.0.0";
  public static final int OZONE_S3G_HTTP_BIND_PORT_DEFAULT = 9878;
  public static final int OZONE_S3G_HTTPS_BIND_PORT_DEFAULT = 9879;

  public static final String OZONE_S3G_DOMAIN_NAME = "ozone.s3g.domain.name";

  public static final String OZONE_S3G_HTTP_AUTH_CONFIG_PREFIX =
      "ozone.s3g.http.auth.";
  public static final String OZONE_S3G_HTTP_AUTH_TYPE =
      OZONE_S3G_HTTP_AUTH_CONFIG_PREFIX + "type";
  public static final String OZONE_S3G_KEYTAB_FILE =
      OZONE_S3G_HTTP_AUTH_CONFIG_PREFIX + "kerberos.keytab";
  public static final String OZONE_S3G_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL =
      OZONE_S3G_HTTP_AUTH_CONFIG_PREFIX + "kerberos.principal";

  public static final String OZONE_S3G_CLIENT_BUFFER_SIZE_KEY =
      "ozone.s3g.client.buffer.size";
  public static final String OZONE_S3G_CLIENT_BUFFER_SIZE_DEFAULT =
      "4KB";

  /**
   * Never constructed.
   */
  private S3GatewayConfigKeys() {

  }
}
