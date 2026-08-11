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

package org.apache.hadoop.ozone.om.multitenant;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_SERVICE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.sun.jersey.api.client.ClientResponse;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Acl;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Policy;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantAccessController.Role;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RangerClientMultiTenantAccessController} that use a
 * mock {@link RangerClient} instead of a live Ranger endpoint.
 */
class TestRangerClientMultiTenantAccessControllerMockClient {

  private RangerClient rangerClient;
  private RangerClientMultiTenantAccessController accessController;

  @BeforeEach
  public void setUpMocks() throws Exception {
    rangerClient = mock(RangerClient.class);
    MutableConfigurationSource conf = new InMemoryConfigurationForTesting();
    conf.set(OZONE_RANGER_HTTPS_ADDRESS_KEY, "https://localhost:6182/");
    conf.set(OZONE_RANGER_SERVICE, "cm_ozone");
    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, "om/_HOST@EXAMPLE.COM");
    conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY, "/path/to/ozone.keytab");

    // Initialize Kerberos name rules before creating the controller
    KerberosName.setRules(
        "RULE:[2:$1@$0](.*@EXAMPLE.COM)s/@.*//\n" +
           "DEFAULT");
    accessController = new RangerClientMultiTenantAccessController(conf);

    Field clientField = RangerClientMultiTenantAccessController.class.getDeclaredField("client");
    clientField.setAccessible(true);
    clientField.set(accessController, rangerClient);
  }

  @Test
  public void testDeleteRoleAbsentRoleRanger28Workaround() throws Exception {
    // Ranger 2.8 returns HTTP 400 with "does not exist" message when role is missing.
    RangerServiceException rse = mock(RangerServiceException.class);
    when(rse.getStatus()).thenReturn(ClientResponse.Status.BAD_REQUEST);
    when(rse.getMessage()).thenReturn("Role with name 'tenant-role' does not exist");

    doThrow(rse).when(rangerClient)
        .deleteRole(anyString(), anyString(), anyString());

    assertDoesNotThrow(() -> accessController.deleteRole("tenant-role"));
  }

  @Test
  public void testDeleteRoleAbsentRoleCaseInsensitive() throws Exception {
    // Verify case-normalization handles uppercase/mixed-case responses.
    RangerServiceException rse = mock(RangerServiceException.class);
    when(rse.getStatus()).thenReturn(ClientResponse.Status.BAD_REQUEST);
    when(rse.getMessage()).thenReturn("ROLE WITH NAME 'tenant-role' DOES NOT EXIST");

    doThrow(rse).when(rangerClient)
        .deleteRole(anyString(), anyString(), anyString());

    assertDoesNotThrow(() -> accessController.deleteRole("tenant-role"));
  }

  @Test
  public void testDeleteRoleUnrelated400Propagates() throws Exception {
    // Unrelated HTTP 400 (e.g. role referenced by policy) MUST propagate.
    RangerServiceException rse = mock(RangerServiceException.class);
    when(rse.getStatus()).thenReturn(ClientResponse.Status.BAD_REQUEST);
    when(rse.getMessage()).thenReturn("Role 'tenant-role' is currently in use by policy 'p1'");

    doThrow(rse).when(rangerClient)
        .deleteRole(anyString(), anyString(), anyString());

    assertThrows(IOException.class, () -> accessController.deleteRole("tenant-role"));
  }

  @Test
  public void testDeleteRoleGenuine404TreatedAsIdempotent() throws Exception {
    // Standard HTTP 404 for missing role should pass silently.
    RangerServiceException rse = mock(RangerServiceException.class);
    when(rse.getStatus()).thenReturn(ClientResponse.Status.NOT_FOUND);

    doThrow(rse).when(rangerClient)
        .deleteRole(anyString(), anyString(), anyString());

    assertDoesNotThrow(() -> accessController.deleteRole("tenant-role"));
  }

  @Test
  public void testCreatePolicyFailFastOnDuplicate() throws Exception {
    // Verify createPolicy fails fast on exception without attempting reconciliation.
    RangerServiceException rse = mock(RangerServiceException.class);
    when(rangerClient.createPolicy(any())).thenThrow(rse);

    Policy policy = new Policy.Builder()
        .setName("tenant-policy")
        .addUserAcl("user",
            Collections.singletonList(Acl.allow(ACLType.READ)))
        .addRoleAcl("role",
            Collections.singletonList(Acl.allow(ACLType.READ)))
        .build();
    assertThrows(IOException.class, () -> accessController.createPolicy(policy));
  }

  @Test
  public void testCreateRoleFailFastOnDuplicate() throws Exception {
    // Verify createRole fails fast on exception without attempting reconciliation.
    RangerServiceException rse = mock(RangerServiceException.class);
    when(rangerClient.createRole(anyString(), any())).thenThrow(rse);

    Role role = new Role.Builder().setName("tenant-role").build();
    assertThrows(IOException.class, () -> accessController.createRole(role));
  }
}
