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

package org.apache.hadoop.ozone.s3.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.s3.endpoint.S3Owner;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests the S3Utils APIs.
 */
public class TestS3Utils {
  private static final ReplicationConfig EC32REPLICATIONCONFIG =
      new ECReplicationConfig(3, 2);
  private static final ReplicationConfig RATIS3REPLICATIONCONFIG =
      RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
  private static final ReplicationConfig RATIS1REPLICATIONCONFIG =
      RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE);

  private static final List<ReplicationConfig> REPLICATIONS = Arrays.asList(
      null,
      RATIS1REPLICATIONCONFIG,
      RATIS3REPLICATIONCONFIG,
      EC32REPLICATIONCONFIG
  );

  private static final List<String> S3STORAGETYPES = Arrays.asList(
      null,
      "",
      S3StorageType.STANDARD.name(),
      S3StorageType.REDUCED_REDUNDANCY.name(),
      S3StorageType.STANDARD_IA.name()
  );

  private static final List<String> S3STORAGECONFIG = Arrays.asList(
      null,
      "",
      "rs-6-3-1024k"
  );

  public static List<Arguments> validS3ReplicationConfigs() {
    List<Arguments> args = new ArrayList<>();
    for (String s3StorageType : S3STORAGETYPES) {
      for (String s3StorageConfig : S3STORAGECONFIG) {
        for (ReplicationConfig clientReplConfig : REPLICATIONS) {
          for (ReplicationConfig bucketReplConfig: REPLICATIONS) {
            args.add(Arguments.of(s3StorageType, s3StorageConfig, clientReplConfig, bucketReplConfig));
          }
        }
      }
    }
    return args;
  }

  @ParameterizedTest
  @MethodSource("validS3ReplicationConfigs")
  public void testValidResolveS3ClientSideReplicationConfig(String s3StorageType, String s3StorageConfig,
      ReplicationConfig clientConfiguredReplConfig, ReplicationConfig bucketReplConfig)
      throws OS3Exception {
    ReplicationConfig replicationConfig = S3Utils
        .resolveS3ClientSideReplicationConfig(s3StorageType, s3StorageConfig,
            clientConfiguredReplConfig, bucketReplConfig);

    final ReplicationConfig expectedReplConfig;
    if (!StringUtils.isEmpty(s3StorageType)) {
      if (S3StorageType.STANDARD_IA.name().equals(s3StorageType)) {
        if (!StringUtils.isEmpty(s3StorageConfig)) {
          expectedReplConfig = new ECReplicationConfig(s3StorageConfig);
        } else {
          expectedReplConfig = EC32REPLICATIONCONFIG;
        }
      } else if (S3StorageType.STANDARD.name().equals(s3StorageType)) {
        expectedReplConfig = RATIS3REPLICATIONCONFIG;
      } else {
        expectedReplConfig = RATIS1REPLICATIONCONFIG;
      }
    } else if (clientConfiguredReplConfig != null) {
      expectedReplConfig = clientConfiguredReplConfig;
    } else if (bucketReplConfig != null) {
      expectedReplConfig = bucketReplConfig;
    } else {
      expectedReplConfig = null;
    }

    if (expectedReplConfig == null) {
      assertNull(replicationConfig);
    } else {
      assertEquals(expectedReplConfig, replicationConfig);
    }
  }

  public static List<Arguments> invalidS3ReplicationConfigs() {
    List<Arguments> args = new ArrayList<>();
    args.add(Arguments.of("GLACIER", null, RATIS3REPLICATIONCONFIG, RATIS1REPLICATIONCONFIG));
    args.add(Arguments.of(S3StorageType.STANDARD_IA.name(), "INVALID",
        RATIS3REPLICATIONCONFIG, RATIS1REPLICATIONCONFIG));
    return args;
  }

  /**
   * When client side passed value also not valid
   * OS3Exception is thrown.
   */
  @ParameterizedTest
  @MethodSource("invalidS3ReplicationConfigs")
  public void testResolveRepConfWhenUserPassedIsInvalid(String s3StorageType, String s3StorageConfig,
      ReplicationConfig clientConfiguredReplConfig, ReplicationConfig bucketReplConfig)
      throws OS3Exception {
    OS3Exception exception = assertThrows(OS3Exception.class, () -> S3Utils.
        resolveS3ClientSideReplicationConfig(
            s3StorageType, s3StorageConfig, clientConfiguredReplConfig, bucketReplConfig));
    assertEquals(S3ErrorTable.INVALID_STORAGE_CLASS.getCode(), exception.getCode());
  }

  @Test
  public void testGenerateCanonicalUserId() {
    assertEquals(S3Owner.DEFAULT_S3OWNER_ID, S3Utils.generateCanonicalUserId("ozone"));
  }

}
