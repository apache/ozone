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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.net.InetAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.Caller;
import org.apache.hadoop.ozone.nfs.protocol.proto.NfsGatewayProtocolProtos.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authorize.DefaultImpersonationProvider;
import org.junit.jupiter.api.Test;

/** Mapping and proxy policy tests; mocked service credentials do not establish a KDC login. */
class TestNfsCallerAuthorizer {
  @Test
  void mapsPrincipalAndEnforcesUsersAndHosts() throws Exception {
    String rules = KerberosName.getRules();
    String mechanism = KerberosName.getRuleMechanism();
    try {
      KerberosName.setRules("RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//");
      KerberosName.setRuleMechanism(KerberosName.MECHANISM_HADOOP);
      UserGroupInformation service = mock(UserGroupInformation.class);
      when(service.getAuthenticationMethod()).thenReturn(AuthenticationMethod.KERBEROS);
      when(service.hasKerberosCredentials()).thenReturn(true);
      when(service.getShortUserName()).thenReturn("nfs-gateway");
      Configuration conf = new Configuration(false);
      conf.set("hadoop.proxyuser.nfs-gateway.users", "alice");
      conf.set("hadoop.proxyuser.nfs-gateway.hosts", "127.0.0.1");
      DefaultImpersonationProvider provider = new DefaultImpersonationProvider();
      provider.setConf(conf);
      provider.init("hadoop.proxyuser");
      NfsCallerAuthorizer authorizer = new NfsCallerAuthorizer(service, provider);
      InetAddress peer = InetAddress.getByName("127.0.0.1");
      for (Caller.Protection protection : Caller.Protection.values()) {
        UserGroupInformation caller = authorizer.authorizeCaller(caller("alice@EXAMPLE.COM", protection), peer);
        assertEquals("alice", caller.getShortUserName());
        assertSame(service, caller.getRealUser());
      }
      assertCode(Status.Code.PERMISSION_DENIED,
          () -> authorizer.authorizeCaller(caller("bob@EXAMPLE.COM", Caller.Protection.PRIVACY), peer));
      assertCode(Status.Code.PERMISSION_DENIED, () -> authorizer.authorizeCaller(
          caller("alice@EXAMPLE.COM", Caller.Protection.PRIVACY), InetAddress.getByName("127.0.0.2")));
      for (String principal : new String[]{"alice", "alice@OTHER.COM", "alice\0@EXAMPLE.COM",
          "a".repeat(1025) + "@EXAMPLE.COM"}) {
        assertCode(Status.Code.UNAUTHENTICATED,
            () -> authorizer.authorizeCaller(caller(principal, Caller.Protection.INTEGRITY), peer));
      }
      assertCode(Status.Code.UNAUTHENTICATED,
          () -> authorizer.authorizeCaller(Caller.newBuilder().setKerberosPrincipal("alice@EXAMPLE.COM")
              .buildPartial(), peer));
      assertCode(Status.Code.UNAUTHENTICATED, () -> authorizer.authorize(RequestContext.getDefaultInstance()));
    } finally {
      KerberosName.setRules(rules);
      KerberosName.setRuleMechanism(mechanism);
    }
  }

  @Test
  void rejectsServiceWithoutKerberosCredentials() {
    assertThrows(IllegalArgumentException.class, () -> new NfsCallerAuthorizer(
        UserGroupInformation.createRemoteUser("nfs-gateway"), new DefaultImpersonationProvider()));
  }

  private static Caller caller(String principal, Caller.Protection protection) {
    return Caller.newBuilder().setKerberosPrincipal(principal).setProtection(protection).build();
  }

  private static void assertCode(Status.Code expected, org.junit.jupiter.api.function.Executable action) {
    assertEquals(expected, assertThrows(StatusRuntimeException.class, action).getStatus().getCode());
  }
}
