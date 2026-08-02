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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.sun.jersey.api.client.ClientResponse;
import java.io.IOException;
import java.lang.reflect.Field;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Unhealthy;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;


@Unhealthy("Requires a Ranger endpoint")
class TestRangerClientMultiTenantAccessController extends MultiTenantAccessControllerTests {

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
    
    accessController = new RangerClientMultiTenantAccessController(conf);

    // Inject mock rangerClient into accessController instance
    Field clientField = RangerClientMultiTenantAccessController.class.getDeclaredField("client");
    clientField.setAccessible(true);
    clientField.set(accessController, rangerClient);
  }

  @Override
  protected MultiTenantAccessController createSubject() {
    MutableConfigurationSource conf = new InMemoryConfigurationForTesting();

    // Set up truststore
    System.setProperty("javax.net.ssl.trustStore",
        "/path/to/cm-auto-global_truststore.jks");

    // Specify Kerberos client config (krb5.conf) path
    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");

    // Enable Kerberos debugging
    System.setProperty("sun.security.krb5.debug", "true");

    // DEFAULT rule uses the default realm configured in krb5.conf
    KerberosName.setRules("DEFAULT");

    conf.set(OZONE_RANGER_HTTPS_ADDRESS_KEY,
        "https://localhost:6182/");

    conf.set(OZONE_RANGER_SERVICE, "cm_ozone");

    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY,
        "om/_HOST@EXAMPLE.COM");

    conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY,
        "/path/to/ozone.keytab");

    GenericTestUtils.setLogLevel(
        LoggerFactory.getLogger(RangerClient.class), Level.DEBUG);

    return assertInstanceOf(RangerClientMultiTenantAccessController.class, MultiTenantAccessController.create(conf));
  }

  @Test
  public void testDeleteRoleAbsentRoleRanger28Workaround() throws Exception {
    // Ranger 2.8 returns HTTP 400 with "does not exist" message when role is missing.
    RangerServiceException rse = mock(RangerServiceException.class);
    when(rse.getStatus()).thenReturn(ClientResponse.Status.BAD_REQUEST);
    when(rse.getMessage()).thenReturn("Role with name 'tenant-role' does not exist");

    doThrow(rse).when(rangerClient)
        .deleteRole(anyString(), anyString(), anyString());

    // Should pass silently (idempotent / tolerant delete)
    assertDoesNotThrow(() -> accessController.deleteRole("tenant-role"));
  }

  @Test
  public void testDeleteRoleAbsentRoleCaseInsensitive() throws Exception {
    // Verify Locale.ROOT case-normalization handles uppercase/mixed-case responses.
    RangerServiceException rse = mock(RangerServiceException.class);
    when(rse.getStatus()).thenReturn(ClientResponse.Status.BAD_REQUEST);
    when(rse.getMessage()).thenReturn("ROLE WITH NAME 'tenant-role' DOES NOT EXIST");

    doThrow(rse).when(rangerClient)
        .deleteRole(anyString(), anyString(), anyString());

    assertDoesNotThrow(() -> accessController.deleteRole("tenant-role"));
  }

  @Test
  public void testDeleteRoleUnrelated400Propagates() throws Exception {
    // Unrelated HTTP 400 (e.g., role is still assigned to an active policy) MUST propagate.
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
    // Verify createPolicy fails fast on exception instead of attempting GET/reconciliation.
    RangerServiceException rse = mock(RangerServiceException.class);
    when(rangerClient.createPolicy(any())).thenThrow(rse);

    Policy policy = new Policy.Builder().setName("tenant-policy").build();
    assertThrows(IOException.class, () -> accessController.createPolicy(policy));
  }

  @Test
  public void testCreateRoleFailFastOnDuplicate() throws Exception {
    // Verify createRole fails fast on exception instead of attempting GET/reconciliation.
    RangerServiceException rse = mock(RangerServiceException.class);
    when(rangerClient.createRole(anyString(), any())).thenThrow(rse);

    Role role = new Role.Builder().setName("tenant-role").build();
    assertThrows(IOException.class, () -> accessController.createRole(role));
  }
}

class TestRangerClientMultiTenantAccessControllerExceptionMapping {
 
  private static final String SERVICE_NAME = "cm_ozone";
  private static final String SHORT_NAME = "ozone";
  private static final String ROLE_NAME = "tenant1-UserRole";
  private static final String POLICY_NAME = "tenant1-VolumeAccess";
 
  private RangerClient rangerClient;
  private RangerClientMultiTenantAccessController controller;
 
  @BeforeEach
  void setup() throws IllegalAccessException {
    rangerClient = mock(RangerClient.class);
 
    // The controller has no test-friendly constructor (it requires a live
    // Kerberos/Ranger ConfigurationSource). Mockito's CALLS_REAL_METHODS
    // mode builds the instance without invoking the constructor, so the
    // real (non-mocked) method bodies under test still run; only the
    // fields the methods depend on are injected via reflection below.
    controller = mock(RangerClientMultiTenantAccessController.class,
        CALLS_REAL_METHODS);
    FieldUtils.writeField(controller, "client", rangerClient, true);
    FieldUtils.writeField(controller, "rangerServiceName", SERVICE_NAME, true);
    FieldUtils.writeField(controller, "shortName", SHORT_NAME, true);
  }
 
  private static RangerServiceException mockException(
      ClientResponse.Status status, String message) {
    RangerServiceException e = mock(RangerServiceException.class);
    org.mockito.Mockito.when(e.getStatus()).thenReturn(status);
    org.mockito.Mockito.when(e.getMessage()).thenReturn(message);
    return e;
  }
 
  @Test
  void deleteRoleSwallowsPlain404() throws Exception {
    RangerServiceException notFound =
        mockException(ClientResponse.Status.NOT_FOUND, "role not found");
    doThrow(notFound).when(rangerClient)
        .deleteRole(anyString(), anyString(), anyString());
 
    assertDoesNotThrow(() -> controller.deleteRole(ROLE_NAME));
    verify(rangerClient, times(1))
        .deleteRole(ROLE_NAME, SHORT_NAME, SERVICE_NAME);
  }
 
  @Test
  void deleteRoleSwallowsRanger28FourHundredNotFoundQuirk() throws Exception {
    RangerServiceException notFound400 = mockException(
        ClientResponse.Status.BAD_REQUEST,
        "Role with name " + ROLE_NAME + " does not exist");
    doThrow(notFound400).when(rangerClient)
        .deleteRole(anyString(), anyString(), anyString());
 
    assertDoesNotThrow(() -> controller.deleteRole(ROLE_NAME));
  }
 
  @Test
  void deleteRolePropagatesUnrelatedFourHundred() throws Exception {
    RangerServiceException stillReferenced = mockException(
        ClientResponse.Status.BAD_REQUEST,
        "Role " + ROLE_NAME + " could not be deleted as it is referenced "
            + "in one or more policies");
    doThrow(stillReferenced).when(rangerClient)
        .deleteRole(anyString(), anyString(), anyString());
 
    assertThrows(IOException.class, () -> controller.deleteRole(ROLE_NAME));
  }
 
  @Test
  void deletePolicySwallows404() throws Exception {
    RangerServiceException notFound =
        mockException(ClientResponse.Status.NOT_FOUND, "policy not found");
    doThrow(notFound).when(rangerClient)
        .deletePolicy(anyString(), anyString());
 
    assertDoesNotThrow(() -> controller.deletePolicy(POLICY_NAME));
  }
 
  @Test
  void deletePolicyDoesNotInheritRoleFourHundredTolerance() throws Exception {
    RangerServiceException badRequest = mockException(
        ClientResponse.Status.BAD_REQUEST,
        "Policy with name " + POLICY_NAME + " does not exist");
    doThrow(badRequest).when(rangerClient)
        .deletePolicy(anyString(), anyString());
 
    assertThrows(IOException.class, () -> controller.deletePolicy(POLICY_NAME));
  }
}
 
