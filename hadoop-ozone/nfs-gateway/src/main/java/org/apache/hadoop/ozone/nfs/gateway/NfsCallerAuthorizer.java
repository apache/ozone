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

import io.grpc.Status;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.Caller;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ImpersonationProvider;

/** Map authenticated Ganesha callers and enforce the configured Hadoop impersonation policy. */
public final class NfsCallerAuthorizer {
  private final UserGroupInformation gatewayUser;
  private final ImpersonationProvider impersonationProvider;

  /** The provider must be initialized with the operator's Hadoop proxy-user configuration. */
  public NfsCallerAuthorizer(UserGroupInformation gatewayUser, ImpersonationProvider impersonationProvider) {
    this.gatewayUser = Objects.requireNonNull(gatewayUser, "gatewayUser");
    this.impersonationProvider = Objects.requireNonNull(impersonationProvider, "impersonationProvider");
    if (gatewayUser.getAuthenticationMethod() != AuthenticationMethod.KERBEROS
        || !gatewayUser.hasKerberosCredentials() || gatewayUser.getRealUser() != null) {
      throw new IllegalArgumentException("NFS backend requires a logged-in Kerberos service user");
    }
  }

  /** Authorize only assertions received from the pinned TLS peer for this export. */
  public UserGroupInformation authorize(RequestContext context) {
    NfsTlsPeerInterceptor.requireExport(context.getExportUuid());
    if (!context.isInitialized() || context.getVersion() != 1) {
      throw Status.INVALID_ARGUMENT.withDescription("Invalid NFS caller context").asRuntimeException();
    }
    return authorizeCaller(context.getCaller(), NfsTlsPeerInterceptor.getPeerAddress().getAddress());
  }

  UserGroupInformation getGatewayUser() {
    return gatewayUser;
  }

  UserGroupInformation authorizeCaller(Caller caller, InetAddress peerAddress) {
    String principal = caller.getKerberosPrincipal();
    if (!caller.isInitialized() || principal.indexOf('\0') >= 0
        || principal.getBytes(StandardCharsets.UTF_8).length > 1024) {
      throw Status.UNAUTHENTICATED.withDescription("Invalid NFS Kerberos principal").asRuntimeException();
    }
    try {
      KerberosName name = new KerberosName(principal);
      if (name.getRealm() == null || name.getRealm().isEmpty()) {
        throw new IOException("A realm-bearing Kerberos principal is required");
      }
      String shortName = name.getShortName();
      if (shortName.isEmpty() || shortName.indexOf('@') >= 0 || shortName.indexOf('/') >= 0) {
        throw new IOException("Kerberos mapping did not produce a local user");
      }
      UserGroupInformation callerUser = UserGroupInformation.createProxyUser(shortName, gatewayUser);
      impersonationProvider.authorize(callerUser, Objects.requireNonNull(peerAddress, "peerAddress"));
      return callerUser;
    } catch (AuthorizationException e) {
      throw Status.PERMISSION_DENIED.withDescription("NFS caller impersonation denied").asRuntimeException();
    } catch (IOException | IllegalArgumentException e) {
      throw Status.UNAUTHENTICATED.withDescription("NFS Kerberos principal mapping failed").asRuntimeException();
    }
  }
}
