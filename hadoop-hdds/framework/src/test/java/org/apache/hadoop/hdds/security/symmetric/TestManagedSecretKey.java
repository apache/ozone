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

package org.apache.hadoop.hdds.security.symmetric;

import static com.google.common.collect.ImmutableSet.of;
import static java.time.Duration.ofDays;
import static java.time.Instant.now;
import static org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey.fromProtobuf;
import static org.apache.hadoop.hdds.security.symmetric.SecretKeyTestUtil.generateHmac;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.junit.jupiter.api.Test;

/**
 * Simple test cases for {@link ManagedSecretKey}.
 */
public class TestManagedSecretKey {

  @Test
  public void testSignAndVerifySuccess() throws Exception {
    // Data can be signed and verified by same key.
    byte[] data = RandomUtils.secure().randomBytes(100);
    ManagedSecretKey secretKey = generateHmac(now(), ofDays(1));
    byte[] signature = secretKey.sign(data);
    assertTrue(secretKey.isValidSignature(data, signature));

    // Data can be signed and verified by same key transferred via network.
    ManagedSecretKey transferredKey = fromProtobuf(secretKey.toProtobuf());
    assertTrue(transferredKey.isValidSignature(data, signature));

    // Token can be sign and verified by the same key.
    OzoneBlockTokenIdentifier tokenId = new OzoneBlockTokenIdentifier("owner",
        new BlockID(1L, 1L), of(AccessModeProto.READ), 0L, 1L);
    tokenId.setSecretKeyId(secretKey.getId());

    signature = secretKey.sign(tokenId);
    assertTrue(secretKey.isValidSignature(tokenId, signature));

    // Token can be signed and verified by same key transferred via network.
    assertTrue(transferredKey.isValidSignature(tokenId, signature));
  }

  @Test
  public void testVerifyFailure() throws Exception {
    byte[] data = RandomUtils.secure().randomBytes(100);
    ManagedSecretKey secretKey = generateHmac(now(), ofDays(1));
    // random signature is not valid.
    assertFalse(secretKey.isValidSignature(data, RandomUtils.secure().randomBytes(100)));

    // Data sign by one key can't be verified by another key.
    byte[] signature = secretKey.sign(data);
    ManagedSecretKey secretKey1 = generateHmac(now(), ofDays(1));
    assertFalse(secretKey1.isValidSignature(data, signature));
  }
}
