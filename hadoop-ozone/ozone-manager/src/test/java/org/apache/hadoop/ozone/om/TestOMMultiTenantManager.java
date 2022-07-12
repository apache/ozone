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
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RANGER_HTTPS_ADMIN_API_USER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_RANGER_SERVICE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FEATURE_NOT_ENABLED;

/**
 * Tests OMMultiTenantManager.
 */
public class TestOMMultiTenantManager {

  /**
   * Try different configs against
   * OMMultiTenantManager#checkAndEnableMultiTenancy and verify its response.
   */
  @Test
  public void testMultiTenancyCheckConfig() {
    final OzoneManager ozoneManager = Mockito.mock(OzoneManager.class);

    final OzoneConfiguration conf = new OzoneConfiguration();

    // Case 1: ozone.om.multitenancy.enabled = false
    conf.setBoolean(OZONE_OM_MULTITENANCY_ENABLED, false);
    Assert.assertFalse(
        OMMultiTenantManager.checkAndEnableMultiTenancy(ozoneManager, conf));

    // Case 2: ozone.om.multitenancy.enabled = true
    // Initially however none of the other essential configs are set.
    conf.setBoolean(OZONE_OM_MULTITENANCY_ENABLED, true);
    expectConfigCheckToFail(ozoneManager, conf);

    // "Enable" security
    Mockito.when(ozoneManager.isSecurityEnabled()).thenReturn(true);
    expectConfigCheckToFail(ozoneManager, conf);

    // Enable Kerberos auth
    conf.set(HADOOP_SECURITY_AUTHENTICATION,
        StringUtils.toLowerCase(AuthenticationMethod.KERBEROS.toString()));
    expectConfigCheckToFail(ozoneManager, conf);

    // Deliberately set ozone.om.kerberos.principal and
    // ozone.om.kerberos.keytab.file to empty values in order to
    // test the config checker, since the default values aren't empty.
    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, "");
    conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY, "");

    // Set essential Ranger conf one by one
    conf.set(OZONE_RANGER_HTTPS_ADDRESS_KEY, "https://ranger:6182");
    expectConfigCheckToFail(ozoneManager, conf);
    conf.set(OZONE_RANGER_SERVICE, "cm_ozone");
    expectConfigCheckToFail(ozoneManager, conf);

    // Try Kerberos auth
    final OzoneConfiguration confKerbAuth = new OzoneConfiguration(conf);
    confKerbAuth.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, "om/_HOST@REALM");
    expectConfigCheckToFail(ozoneManager, confKerbAuth);
    confKerbAuth.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY, "/path/to/om.keytab");
    Assert.assertTrue(OMMultiTenantManager.checkAndEnableMultiTenancy(
        ozoneManager, confKerbAuth));

    // Try basic auth
    final OzoneConfiguration confBasicAuth = new OzoneConfiguration(conf);
    confBasicAuth.set(OZONE_OM_RANGER_HTTPS_ADMIN_API_USER, "admin");
    expectConfigCheckToFail(ozoneManager, confBasicAuth);
    confBasicAuth.set(OZONE_OM_RANGER_HTTPS_ADMIN_API_PASSWD, "Password1");
    // At this point the config check should pass. Method returns true
    Assert.assertTrue(OMMultiTenantManager.checkAndEnableMultiTenancy(
        ozoneManager, confBasicAuth));
  }

  /**
   * Helper function for testMultiTenancyConfig.
   */
  private void expectConfigCheckToFail(OzoneManager ozoneManager,
      OzoneConfiguration conf) {
    try {
      OMMultiTenantManager.checkAndEnableMultiTenancy(ozoneManager, conf);
      Assert.fail("Should have thrown RuntimeException");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("Failed to meet"));
    }
  }

  /**
   * Verify that Multi-Tenancy read and write requests are blocked as intended
   * when the the feature is disabled.
   */
  @Test
  public void testMultiTenancyRequestsWhenDisabled() throws IOException {

    final OzoneManager ozoneManager = Mockito.mock(OzoneManager.class);
    Mockito.doCallRealMethod().when(ozoneManager).checkS3MultiTenancyEnabled();

    Mockito.when(ozoneManager.isS3MultiTenancyEnabled()).thenReturn(false);

    final String tenantId = "test-tenant";
    final String userPrincipal = "alice";
    final String accessId =
        OMMultiTenantManager.getDefaultAccessId(tenantId, userPrincipal);

    // Check that Multi-Tenancy write requests are blocked when not enabled
    expectWriteRequestToFail(ozoneManager,
        OMRequestTestUtils.createTenantRequest(tenantId));
    expectWriteRequestToFail(ozoneManager,
        OMRequestTestUtils.deleteTenantRequest(tenantId));
    expectWriteRequestToFail(ozoneManager,
        OMRequestTestUtils.tenantAssignUserAccessIdRequest(
            userPrincipal, tenantId, accessId));
    expectWriteRequestToFail(ozoneManager,
        OMRequestTestUtils.tenantRevokeUserAccessIdRequest(accessId));
    expectWriteRequestToFail(ozoneManager,
        OMRequestTestUtils.tenantAssignAdminRequest(accessId, tenantId, true));
    expectWriteRequestToFail(ozoneManager,
        OMRequestTestUtils.tenantRevokeAdminRequest(accessId, tenantId));

    // Check that Multi-Tenancy read requests are blocked when not enabled
    final OzoneManagerRequestHandler ozoneManagerRequestHandler =
        new OzoneManagerRequestHandler(ozoneManager, null);

    expectReadRequestToFail(ozoneManagerRequestHandler,
        OMRequestTestUtils.listUsersInTenantRequest(tenantId));
    expectReadRequestToFail(ozoneManagerRequestHandler,
        OMRequestTestUtils.listTenantRequest());
    expectReadRequestToFail(ozoneManagerRequestHandler,
        OMRequestTestUtils.tenantGetUserInfoRequest(tenantId));

    // getS3VolumeContext request does not throw exception when MT is disabled.
    // Rather, it falls back to the default s3v for backwards compatibility.
  }

  /**
   * Helper function for testMultiTenancyRPCWhenDisabled.
   */
  private void expectWriteRequestToFail(OzoneManager om, OMRequest omRequest)
      throws IOException {
    try {
      OzoneManagerRatisUtils.createClientRequest(omRequest, om);
      Assert.fail("Should have thrown OMException");
    } catch (OMException e) {
      Assert.assertEquals(FEATURE_NOT_ENABLED, e.getResult());
    }
  }

  /**
   * Helper function for testMultiTenancyRPCWhenDisabled.
   */
  private void expectReadRequestToFail(OzoneManagerRequestHandler handler,
      OMRequest omRequest) {

    // handleReadRequest does not throw
    OMResponse omResponse = handler.handleReadRequest(omRequest);
    Assert.assertFalse(omResponse.getSuccess());
    Assert.assertEquals(Status.FEATURE_NOT_ENABLED, omResponse.getStatus());
    Assert.assertTrue(omResponse.getMessage()
        .startsWith("S3 multi-tenancy feature is not enabled"));
  }

}
