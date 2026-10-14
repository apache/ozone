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

package org.apache.hadoop.ozone.nfs.gateway;

/** Configuration keys for the standalone NFS gateway process. */
public final class NfsGatewayConfigKeys {
  public static final String ADDRESS = "ozone.nfs.gateway.address";
  public static final String PORT = "ozone.nfs.gateway.port";
  public static final String SERVER_CERTIFICATE = "ozone.nfs.gateway.server.certificate.file";
  public static final String SERVER_PRIVATE_KEY = "ozone.nfs.gateway.server.private.key.file";
  public static final String TRUSTED_CA = "ozone.nfs.gateway.trusted.ca.file";
  public static final String GANESHA_CLIENT_CERTIFICATES = "ozone.nfs.gateway.ganesha.cert.sha256";
  public static final String KERBEROS_PRINCIPAL = "ozone.nfs.gateway.kerberos.principal";
  public static final String KERBEROS_KEYTAB = "ozone.nfs.gateway.kerberos.keytab.file";
  public static final String GATEWAY_ID = "ozone.nfs.gateway.id";
  public static final String EXPORT_LEASE_DURATION = "ozone.nfs.gateway.export.lease.duration";
  public static final String STAGING_VOLUME = "ozone.nfs.gateway.staging.volume";
  public static final String STAGING_BUCKET = "ozone.nfs.gateway.staging.bucket";

  private NfsGatewayConfigKeys() {
  }
}
