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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.REVOKED_TOKEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.security.STSTokenSecretManager.CreateSTSTokenParams;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.KerberosTests;
import org.apache.ozone.test.LambdaTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Tests S3 secret revocation through secure OM HA RPC, failover and restart. */
class TestS3SecretRevocationHA extends KerberosTests {
  private static final String VOLUME = "sts-revocation";
  private static final String STRING_TO_SIGN = "AWS4-HMAC-SHA256\n20150830T123600Z\n" +
      "20150830/us-east-1/iam/aws4_request\n" +
      "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59";
  private static final String SIGNATURE = "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7";
  private static final String SECRET = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

  private MiniOzoneHAClusterImpl cluster;

  @AfterEach
  void cleanup() {
    try {
      if (cluster != null) {
        cluster.shutdown();
      }
    } finally {
      stopMiniKdc();
    }
  }

  @Test
  void testRevokedTokenStaysRevokedAfterSecretRecreationAndFailover() throws Exception {
    ExitUtils.disableSystemExit();
    initKerberos();
    getConf().set(OZONE_ADMINISTRATORS, "scm");
    getConf().setBoolean(HDDS_BLOCK_TOKEN_ENABLED, true);
    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(getConf())
        .setSCMServiceId("sts-scm")
        .setNumOfStorageContainerManagers(1)
        .setOMServiceId("sts-revocation")
        .setNumOfOzoneManagers(3);
    builder.withoutDatanodes();
    cluster = builder.build();
    cluster.waitForClusterToBeReady();
    OzoneManager leader = cluster.waitForLeaderOM();
    assertTrue(leader.isSecurityEnabled());

    try (OzoneClient client = cluster.newClient()) {
      String accessId = UserGroupInformation.getCurrentUser().getUserName();
      client.getObjectStore().createVolume(VOLUME);
      client.getObjectStore().getS3Secret(accessId);
      Instant tokenCreated = Instant.now();
      S3Auth oldToken = createToken(leader, accessId, tokenCreated);
      assertTokenAccepted(client, oldToken);

      // The cutoff comparison is strict: ensure the token predates the revoke stamp.
      GenericTestUtils.waitFor(() -> Instant.now().toEpochMilli() > tokenCreated.toEpochMilli(), 10, 5000);
      client.getObjectStore().revokeS3Secret(accessId);
      assertTokenRevoked(client, oldToken);
      client.getObjectStore().getS3Secret(accessId);
      assertTokenRevoked(client, oldToken);

      Long cutoff = leader.getMetadataManager().getS3RevokedStsTokenTable().get(accessId);
      assertNotNull(cutoff);
      GenericTestUtils.waitFor(() -> System.currentTimeMillis() > cutoff, 10, 5000);
      S3Auth newToken = createToken(leader, accessId, Instant.now());
      assertTokenAccepted(client, newToken);

      // All replicas must persist the same cutoff, independently of their table caches.
      for (OzoneManager om : cluster.getOzoneManagersList()) {
        LambdaTestUtils.await(30000, 100, () -> cutoff.equals(
            om.getMetadataManager().getS3RevokedStsTokenTable().getSkipCache(accessId)));
      }
      String oldLeaderId = leader.getOMNodeId();
      cluster.stopOzoneManager(oldLeaderId);
      OzoneManager newLeader = cluster.waitForLeaderOM();
      assertNotEquals(oldLeaderId, newLeader.getOMNodeId());
      assertTokenRevoked(client, oldToken);
      assertTokenAccepted(client, newToken);

      cluster.restartOzoneManager();
      cluster.waitForLeaderOM();
      assertTokenRevoked(client, oldToken);
      assertTokenAccepted(client, newToken);
    }
  }

  private S3Auth createToken(OzoneManager leader, String accessId, Instant creationTime) throws Exception {
    // Use the real signing/encryption manager; Ranger AssumeRole authorization is outside this test.
    String temporaryAccessId = "STS" + creationTime.toEpochMilli();
    String token = leader.getSTSTokenSecretManager().createSTSTokenString(CreateSTSTokenParams.newBuilder()
        .setTempAccessKeyId(temporaryAccessId)
        .setOriginalAccessKeyId(accessId)
        .setRoleArn("arn:aws:iam::123456789012:role/test-role")
        .setDurationSeconds(900)
        .setSecretAccessKey(SECRET)
        .setSessionPolicy("{}")
        .setAssumedRoleId("test-role:test-session")
        .setAssumedRoleUserArn("arn:aws:sts::123456789012:assumed-role/test-role/test-session")
        .setCreationTime(creationTime)
        .build());
    S3Auth auth = new S3Auth(STRING_TO_SIGN, SIGNATURE, temporaryAccessId, accessId);
    auth.setSessionToken(token);
    return auth;
  }

  private void assertTokenAccepted(OzoneClient client, S3Auth auth) throws Exception {
    client.getProxy().setThreadLocalS3Auth(auth);
    try {
      assertEquals(VOLUME, client.getObjectStore().getVolume(VOLUME).getName());
    } finally {
      client.getProxy().clearThreadLocalS3Auth();
    }
  }

  private void assertTokenRevoked(OzoneClient client, S3Auth auth) {
    client.getProxy().setThreadLocalS3Auth(auth);
    try {
      OMException exception = assertThrows(OMException.class, () -> client.getObjectStore().getVolume(VOLUME));
      assertEquals(REVOKED_TOKEN, exception.getResult());
    } finally {
      client.getProxy().clearThreadLocalS3Auth();
    }
  }
}
