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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Unhealthy;
import org.apache.ranger.RangerClient;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

@Unhealthy("Requires a Ranger endpoint")
class TestRangerClientMultiTenantAccessController extends MultiTenantAccessControllerTests {

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

    // These config keys must be properly set when the test is run:
    //
    // OZONE_RANGER_HTTPS_ADDRESS_KEY
    // OZONE_RANGER_SERVICE
    // OZONE_OM_KERBEROS_PRINCIPAL_KEY
    // OZONE_OM_KERBEROS_KEYTAB_FILE_KEY

    // Same as OM ranger-ozone-security.xml ranger.plugin.ozone.policy.rest.url
    conf.set(OZONE_RANGER_HTTPS_ADDRESS_KEY,
        "https://localhost:6182/");

    // Same as OM ranger-ozone-security.xml ranger.plugin.ozone.service.name
    conf.set(OZONE_RANGER_SERVICE, "cm_ozone");

    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY,
        "om/_HOST@EXAMPLE.COM");

    conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY,
        "/path/to/ozone.keytab");

    // TODO: Test with clear text username and password as well.
//    conf.set(OZONE_OM_RANGER_HTTPS_ADMIN_API_USER, "rangeruser");
//    conf.set(OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD, "passwd");

    // (Optional) Enable RangerClient debug log
    GenericTestUtils.setLogLevel(
        LoggerFactory.getLogger(RangerClient.class), Level.DEBUG);

    return assertInstanceOf(RangerClientMultiTenantAccessController.class, MultiTenantAccessController.create(conf));
  }

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
