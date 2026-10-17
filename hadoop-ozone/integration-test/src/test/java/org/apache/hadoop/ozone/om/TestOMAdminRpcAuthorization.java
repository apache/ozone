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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.security.SecurityConfig.OZONE_TEST_AUTHORIZATION_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.UUID;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.Server;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.protocol.OMConfiguration;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolClientSideImpl;
import org.apache.hadoop.ozone.om.protocolPB.OMInterServiceProtocolClientSideImpl;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EchoRPCRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SafeMode;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetSafeModeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.ClientId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests admin authorization of OM administrative RPCs that are only ever driven by admin
 * CLI tooling: refetchSecretKey() on the client protocol (mirrors the checkAdminUserPrivilege
 * gate used by transferLeadership/listOpenFiles), and getOMConfiguration() on the OM admin
 * protocol (mirrors the isAdmin check used by decommission()). Both must reject non-admin
 * callers and serve admins. bootstrap() on the OM inter service protocol carries the same
 * isAdmin check and must reject non-admin callers.
 */
public class TestOMAdminRpcAuthorization {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static UUID secretKeyId;

  @BeforeAll
  static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OZONE_ACL_AUTHORIZER_CLASS, OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    conf.set(OZONE_ADMINISTRATORS, "admin");
    // Make admin authorization effective without a KDC so the gate actually runs.
    conf.setBoolean(OZONE_TEST_AUTHORIZATION_ENABLED, true);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();

    // A non-secure MiniOzoneCluster does not initialize a real secret key client, so inject a
    // stub whose current key id is stable. The admin-authorization gate runs before this is
    // touched, so only the admin-allowed path exercises it.
    secretKeyId = UUID.randomUUID();
    ManagedSecretKey managedSecretKey = mock(ManagedSecretKey.class);
    when(managedSecretKey.getId()).thenReturn(secretKeyId);
    SecretKeyClient secretKeyClient = mock(SecretKeyClient.class);
    when(secretKeyClient.getCurrentSecretKey()).thenReturn(managedSecretKey);
    cluster.getOzoneManager().setSecretKeyClient(secretKeyClient);
  }

  @AfterAll
  static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static UUID refetchSecretKey(UserGroupInformation user) throws Exception {
    return user.doAs((PrivilegedExceptionAction<UUID>) () -> {
      OzoneManagerProtocolClientSideTranslatorPB omClient =
          new OzoneManagerProtocolClientSideTranslatorPB(
              OmTransportFactory.create(conf, user, null),
              ClientId.randomId().toString());
      return omClient.refetchSecretKey();
    });
  }

  private static OMConfiguration getOMConfiguration(UserGroupInformation user)
      throws Exception {
    return user.doAs((PrivilegedExceptionAction<OMConfiguration>) () -> {
      OMAdminProtocolClientSideImpl adminClient =
          OMAdminProtocolClientSideImpl.createProxyForSingleOM(
              conf, user, cluster.getOzoneManager().getNodeDetails());
      return adminClient.getOMConfiguration();
    });
  }

  private static void bootstrap(UserGroupInformation user) throws Exception {
    OzoneConfiguration bootstrapConf = new OzoneConfiguration(conf);
    bootstrapConf.setInt(OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, 1);
    user.doAs((PrivilegedExceptionAction<Void>) () -> {
      try (OMInterServiceProtocolClientSideImpl interServiceClient =
               new OMInterServiceProtocolClientSideImpl(bootstrapConf, user, null)) {
        // Use the existing OM details to exercise authorization without introducing another OM.
        interServiceClient.bootstrap(cluster.getOzoneManager().getNodeDetails());
      }
      return null;
    });
  }

  private static boolean setSafeMode(UserGroupInformation user, SafeModeAction action) throws Exception {
    return user.doAs((PrivilegedExceptionAction<Boolean>) () -> {
      try (OzoneManagerProtocolClientSideTranslatorPB omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          OmTransportFactory.create(conf, user, null), ClientId.randomId().toString())) {
        return omClient.setSafeMode(action, false);
      }
    });
  }

  @Test
  void forceExitSafeModeNonAdminIsDenied() throws Exception {
    UserGroupInformation nonAdmin = UserGroupInformation.createUserForTesting("nonadmin", new String[] {"users"});
    OMException ex = assertThrows(OMException.class, () -> setSafeMode(nonAdmin, SafeModeAction.FORCE_EXIT));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());
    // Querying safe mode stays open to every user.
    assertFalse(setSafeMode(nonAdmin, SafeModeAction.GET));
  }

  @Test
  void forceExitSafeModeAdminIsAllowed() throws Exception {
    UserGroupInformation admin = UserGroupInformation.createUserForTesting("admin", new String[] {"admins"});
    assertTrue(setSafeMode(admin, SafeModeAction.FORCE_EXIT));
  }

  @Test
  void refetchSecretKeyNonAdminIsDenied() {
    UserGroupInformation nonAdmin =
        UserGroupInformation.createUserForTesting("nonadmin", new String[] {"users"});
    OMException ex = assertThrows(OMException.class, () -> refetchSecretKey(nonAdmin));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());
  }

  @Test
  void refetchSecretKeyAdminIsAllowed() throws Exception {
    UserGroupInformation admin =
        UserGroupInformation.createUserForTesting("admin", new String[] {"admins"});
    assertEquals(secretKeyId, refetchSecretKey(admin));
  }

  @Test
  void getOMConfigurationNonAdminIsDenied() throws Exception {
    UserGroupInformation nonAdmin =
        UserGroupInformation.createUserForTesting("nonadmin", new String[] {"users"});
    OMConfiguration omConfig = getOMConfiguration(nonAdmin);
    // The client swallows a denied (success=false) response into an empty configuration, so a
    // denied caller sees no ring membership at all.
    assertTrue(omConfig == null || omConfig.getCurrentPeerList().isEmpty());
  }

  @Test
  void getOMConfigurationAdminIsAllowed() throws Exception {
    UserGroupInformation admin =
        UserGroupInformation.createUserForTesting("admin", new String[] {"admins"});
    OMConfiguration omConfig = getOMConfiguration(admin);
    assertFalse(omConfig.getCurrentPeerList().isEmpty());
  }

  @Test
  void listTenantWithoutRpcUserIsDenied() {
    OMException ex = assertThrows(OMException.class, () -> cluster.getOzoneManager().listTenant());
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, ex.getResult());
  }

  @Test
  void listTenantAdminIsAllowed() throws Exception {
    Server.Call call = mock(Server.Call.class);
    when(call.getRemoteUser())
        .thenReturn(UserGroupInformation.createUserForTesting("admin", new String[] {"admins"}));
    Server.getCurCall().set(call);
    try {
      assertThat(cluster.getOzoneManager().listTenant().getTenantStateList()).isEmpty();
    } finally {
      Server.getCurCall().remove();
    }
  }

  @Test
  void bootstrapNonAdminIsDenied() {
    UserGroupInformation nonAdmin =
        UserGroupInformation.createUserForTesting("nonadmin", new String[] {"users"});
    assertThatThrownBy(() -> bootstrap(nonAdmin))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Only administrators are authorized to perform bootstrap.");
  }

  private static int echoRPC(UserGroupInformation user, boolean writeToRatis) throws Exception {
    return user.doAs((PrivilegedExceptionAction<Integer>) () -> {
      try (OzoneManagerProtocolClientSideTranslatorPB omClient = new OzoneManagerProtocolClientSideTranslatorPB(
          OmTransportFactory.create(conf, user, null), ClientId.randomId().toString())) {
        return omClient.echoRPCReq(new byte[0], 16, writeToRatis).getPayload().size();
      }
    });
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void echoRPCNonAdminIsDenied(boolean writeToRatis) {
    UserGroupInformation nonAdmin = UserGroupInformation.createUserForTesting("nonadmin", new String[] {"users"});
    OMException ex = assertThrows(OMException.class, () -> echoRPC(nonAdmin, writeToRatis));
    assertEquals(OMException.ResultCodes.ACCESS_DENIED, ex.getResult());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void echoRPCAdminIsAllowed(boolean writeToRatis) throws Exception {
    UserGroupInformation admin = UserGroupInformation.createUserForTesting("admin", new String[] {"admins"});
    assertEquals(16, echoRPC(admin, writeToRatis));
  }

  /** A call without an RPC user, as on the OM gRPC endpoint, must not be treated as the OM user. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void echoRPCWithoutRpcUserIsDenied(boolean writeToRatis) throws Exception {
    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.EchoRPC)
        .setClientId(ClientId.randomId().toString())
        .setEchoRPCRequest(EchoRPCRequest.newBuilder().setPayloadSizeResp(16).setReadOnly(!writeToRatis))
        .build();

    // The call context that OzoneManagerServiceGrpc sets up: a client ID, but no connection and so no user.
    Server.getCurCall().set(new Server.Call(1, 1, null, null, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        ClientId.randomId().toByteString().toByteArray()));
    try {
      OMResponse response = cluster.getOzoneManager().getOmServerProtocol().submitRequest(null, request);
      assertEquals(Status.ACCESS_DENIED, response.getStatus());
    } finally {
      Server.getCurCall().remove();
    }
  }

  @Test
  void forceExitSafeModeWithoutRpcUserIsDenied() throws Exception {
    OMRequest request = OMRequest.newBuilder()
        .setCmdType(Type.SetSafeMode)
        .setClientId(ClientId.randomId().toString())
        .setSetSafeModeRequest(SetSafeModeRequest.newBuilder().setSafeMode(SafeMode.FORCE_EXIT))
        .build();

    // The call context that OzoneManagerServiceGrpc sets up: a client ID, but no connection and so no user.
    Server.getCurCall().set(new Server.Call(1, 1, null, null, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        ClientId.randomId().toByteString().toByteArray()));
    try {
      OMResponse response = cluster.getOzoneManager().getOmServerProtocol().submitRequest(null, request);
      assertEquals(Status.PERMISSION_DENIED, response.getStatus());
    } finally {
      Server.getCurCall().remove();
    }
  }
}
