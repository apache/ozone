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

import static org.apache.hadoop.hdds.client.ReplicationType.EC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.junit.jupiter.api.Test;

/**
 * Test bucket property arguments.
 */
public class TestOmBucketArgs {

  @Test
  public void testQuotaIsSetFlagsAreCorrectlySet() {
    OmBucketArgs bucketArgs = OmBucketArgs.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .build();

    assertFalse(bucketArgs.hasQuotaInBytes());
    assertFalse(bucketArgs.hasQuotaInNamespace());

    OmBucketArgs argsFromProto = OmBucketArgs.getFromProtobuf(
        bucketArgs.getProtobuf());

    assertFalse(argsFromProto.hasQuotaInBytes());
    assertFalse(argsFromProto.hasQuotaInNamespace());

    bucketArgs = OmBucketArgs.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .setQuotaInNamespace(123)
        .setQuotaInBytes(456)
        .build();

    assertTrue(bucketArgs.hasQuotaInBytes());
    assertTrue(bucketArgs.hasQuotaInNamespace());

    argsFromProto = OmBucketArgs.getFromProtobuf(
        bucketArgs.getProtobuf());

    assertTrue(argsFromProto.hasQuotaInBytes());
    assertTrue(argsFromProto.hasQuotaInNamespace());
  }

  @Test
  public void testDefaultReplicationConfigIsSetCorrectly() {
    OmBucketArgs bucketArgs = OmBucketArgs.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .build();

    OmBucketArgs argsFromProto = OmBucketArgs.getFromProtobuf(
        bucketArgs.getProtobuf());

    assertNull(argsFromProto.getDefaultReplicationConfig());

    bucketArgs = OmBucketArgs.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .setDefaultReplicationConfig(new DefaultReplicationConfig(
            new ECReplicationConfig(3, 2)))
        .build();

    argsFromProto = OmBucketArgs.getFromProtobuf(
        bucketArgs.getProtobuf());

    assertEquals(EC,
        argsFromProto.getDefaultReplicationConfig().getType());
  }

  @Test
  public void corsConfigurationCanBeSetOrCleared() {
    CorsConfiguration corsConfiguration =
        CorsConfiguration.newBuilder()
            .addRule(CorsRule.newBuilder()
                .setAllowedOrigins(Collections.singletonList("*"))
                .setAllowedMethods(Collections.singletonList("GET"))
                .build())
            .build();

    OmBucketArgs setArgs = OmBucketArgs.newBuilder()
        .setVolumeName("vol1")
        .setBucketName("bucket")
        .setCorsConfiguration(corsConfiguration)
        .build();

    OmBucketArgs recoveredSetArgs =
        OmBucketArgs.getFromProtobuf(setArgs.getProtobuf());
    assertTrue(recoveredSetArgs.hasCorsConfiguration());
    assertFalse(recoveredSetArgs.shouldClearCorsConfiguration());
    assertEquals(corsConfiguration, recoveredSetArgs.getCorsConfiguration());

    OmBucketArgs clearArgs = OmBucketArgs.newBuilder()
        .setVolumeName("vol1")
        .setBucketName("bucket")
        .setClearCorsConfiguration(true)
        .build();

    OmBucketArgs recoveredClearArgs =
        OmBucketArgs.getFromProtobuf(clearArgs.getProtobuf());
    assertFalse(recoveredClearArgs.hasCorsConfiguration());
    assertTrue(recoveredClearArgs.shouldClearCorsConfiguration());
  }
}
