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
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3G_STS_HTTP_ENABLED_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTITENANCY_ENABLED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.REVOKED_TOKEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.SecretKeyTestClient;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.AssumeRoleResponseInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.security.HmacSha256;
import org.apache.hadoop.ozone.security.STSTokenSecretManager;
import org.apache.hadoop.ozone.security.acl.AssumeRoleRequest;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** Tests tenant STS revocation over RPC with replicated OM metadata and leader failover. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOzoneManagerHATenantStsRevocation extends AbstractOzoneManagerHATest {
  private static final String STRING_TO_SIGN = "AWS4-HMAC-SHA256\n20150830T123600Z\n"
      + "20150830/us-east-1/s3/aws4_request\n" + "0".repeat(64);

  @BeforeAll
  public void init() throws Exception {
    // Use the secure OM test hook used by TestSecureOzoneRpcClient, without an external KDC or Ranger.
    OzoneManager.setTestSecureOmFlag(true);
    initCluster(false, conf -> {
      conf.setBoolean(OZONE_TEST_AUTHORIZATION_ENABLED, true);
      conf.setBoolean(OZONE_OM_MULTITENANCY_ENABLED, true);
      conf.setBoolean(OZONE_S3G_STS_HTTP_ENABLED_KEY, true);
      conf.setBoolean(OMMultiTenantManagerImpl.OZONE_OM_TENANT_DEV_SKIP_RANGER, true);
      conf.set(OZONE_ACL_AUTHORIZER_CLASS, TestAuthorizer.class.getName());
    });
    SecretKeyTestClient keys = new SecretKeyTestClient();
    for (OzoneManager om : getCluster().getOzoneManagersList()) {
      om.setCertClient(new CertificateClientTestImpl(om.getConfiguration()));
      om.setSecretKeyClient(keys);
      // STSTokenSecretManager keeps its own final signer reference in addition to the inherited one.
      HddsWhiteboxTestUtils.setInternalState(om, "stsTokenSecretManager", new STSTokenSecretManager(keys));
      om.startSecretManager();
    }
  }

  @AfterAll
  public static void resetSecureOmFlag() {
    OzoneManager.setTestSecureOmFlag(false);
  }

  @Test
  public void testRevokedTenantTokenStaysRevokedAfterReassignmentAndFailover() throws Exception {
    String tenant = "tenant-" + UUID.randomUUID();
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    String accessId = tenant + "$" + user;
    ObjectStore store = getObjectStore();
    ClientProtocol proxy = store.getClientProxy();
    store.createTenant(tenant);
    S3SecretValue original = store.tenantAssignUserAccessId(user, tenant, accessId);
    AssumeRoleResponseInfo oldToken;
    try {
      proxy.setThreadLocalS3Auth(auth(accessId, original.getAwsSecret(), user, null));
      oldToken = store.assumeRole("arn:aws:iam::123456789012:role/test-role", "before-revoke", 900,
          null, UUID.randomUUID().toString());
      proxy.setThreadLocalS3Auth(auth(oldToken.getAccessKeyId(), oldToken.getSecretAccessKey(), user,
          oldToken.getSessionToken()));
      assertEquals(tenant, store.getVolume(tenant).getName());
    } finally {
      proxy.clearThreadLocalS3Auth();
    }

    store.tenantRevokeUserAccessId(accessId);
    Long cutoff = getCluster().waitForLeaderOM().getMetadataManager().getS3RevokedStsTokenTable().get(accessId);
    assertNotNull(cutoff);
    for (OzoneManager om : getCluster().getOzoneManagersList()) {
      GenericTestUtils.waitFor(() -> {
        try {
          return cutoff.equals(om.getMetadataManager().getS3RevokedStsTokenTable().getSkipCache(accessId));
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }, 100, 30000);
    }
    S3SecretValue replacement = store.tenantAssignUserAccessId(user, tenant, accessId);
    assertRevoked(store, oldToken, user, tenant);

    OzoneManager oldLeader = getCluster().waitForLeaderOM();
    getCluster().stopOzoneManager(oldLeader.getOMNodeId());
    OzoneManager newLeader = getCluster().waitForLeaderOM();
    assertEquals(cutoff, newLeader.getMetadataManager().getS3RevokedStsTokenTable().get(accessId));
    assertRevoked(store, oldToken, user, tenant);
    assertTrue(oldToken.getExpirationEpochSeconds() > Instant.now().getEpochSecond());

    try {
      proxy.setThreadLocalS3Auth(auth(accessId, replacement.getAwsSecret(), user, null));
      AssumeRoleResponseInfo newToken = store.assumeRole("arn:aws:iam::123456789012:role/test-role",
          "after-revoke", 900, null, UUID.randomUUID().toString());
      proxy.setThreadLocalS3Auth(auth(newToken.getAccessKeyId(), newToken.getSecretAccessKey(), user,
          newToken.getSessionToken()));
      assertEquals(tenant, store.getVolume(tenant).getName());
    } finally {
      proxy.clearThreadLocalS3Auth();
    }
  }

  private void assertRevoked(ObjectStore store, AssumeRoleResponseInfo token, String user, String tenant)
      throws Exception {
    try {
      store.getClientProxy().setThreadLocalS3Auth(auth(token.getAccessKeyId(), token.getSecretAccessKey(), user,
          token.getSessionToken()));
      OMException ex = assertThrows(OMException.class, () -> store.getVolume(tenant));
      assertEquals(REVOKED_TOKEN, ex.getResult());
    } finally {
      store.getClientProxy().clearThreadLocalS3Auth();
    }
  }

  private static S3Auth auth(String accessId, String secret, String user, String token) throws Exception {
    byte[] key = AWSV4AuthValidator.getSigningKey(secret, STRING_TO_SIGN);
    String signature = Hex.encodeHexString(
        HmacSha256.sign(new SecretKeySpec(key, HmacSha256.ALGORITHM), STRING_TO_SIGN));
    S3Auth auth = new S3Auth(STRING_TO_SIGN, signature, accessId, user);
    auth.setSessionToken(token);
    return auth;
  }

  /** Supplies a session policy without requiring an external Ranger service. */
  public static class TestAuthorizer implements IAccessAuthorizer {
    @Override
    public boolean checkAccess(IOzoneObj obj, RequestContext context) {
      return true;
    }

    @Override
    public String generateAssumeRoleSessionPolicy(AssumeRoleRequest request) {
      return "{}";
    }
  }
}
