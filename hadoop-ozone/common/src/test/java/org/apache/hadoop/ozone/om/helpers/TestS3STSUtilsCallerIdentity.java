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

package org.apache.hadoop.ozone.om.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Test caller identity resolution helpers in S3STSUtils.
 */
public class TestS3STSUtilsCallerIdentity {

  private static final String PRINCIPAL = "om/polarisclient@example.com";
  private static final String KERBEROS_SHORT_NAME = "om";
  private static final String ASSUMED_ROLE_ID = "AROATEST123456789:testsess";
  private static final String ASSUMED_ROLE_USER_ARN =
      "arn:aws:sts::123456789012:assumed-role/test-role/testsess";

  @Test
  public void testToIamUserArn() {
    assertEquals("arn:aws:iam::123456789012:user/om", S3STSUtils.toIamUserArn(KERBEROS_SHORT_NAME));
  }

  @Test
  public void testResolveCallerIdentityForPermanentCredentials() {
    final CallerIdentityInfo identity = S3STSUtils.resolveCallerIdentityForPermanentCredentials(
        PRINCIPAL, KERBEROS_SHORT_NAME);

    assertEquals(S3STSUtils.OZONE_STATIC_ACCOUNT_ID, identity.getAccount());
    assertEquals("arn:aws:iam::123456789012:user/om", identity.getArn());
    assertEquals(PRINCIPAL, identity.getUserId());
  }

  @Test
  public void testResolveCallerIdentityForStsCredentials() {
    final CallerIdentityInfo identity = S3STSUtils.resolveCallerIdentityForStsCredentials(
        ASSUMED_ROLE_ID, ASSUMED_ROLE_USER_ARN);

    assertEquals(S3STSUtils.OZONE_STATIC_ACCOUNT_ID, identity.getAccount());
    assertEquals(ASSUMED_ROLE_USER_ARN, identity.getArn());
    assertEquals(ASSUMED_ROLE_ID, identity.getUserId());
  }
}
