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

import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_STORAGE_CLASS_REDUCED_REDUNDANCY_STORAGE_POLICY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_STORAGE_CLASS_STANDARD_STORAGE_POLICY;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_GLACIER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_REDUCED_REDUNDANCY;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_STANDARD;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_STANDARD_IA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.client.StoragePolicy;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.junit.jupiter.api.Test;

/** Tests the S3 storage class mapping to Ozone replication and storage policy. */
class TestS3StorageClass {

  private static final ReplicationConfig RATIS_ONE =
      RatisReplicationConfig.getInstance(ReplicationFactor.ONE);
  private static final ReplicationConfig RATIS_THREE =
      RatisReplicationConfig.getInstance(ReplicationFactor.THREE);
  private static final ReplicationConfig STANDALONE_ONE =
      StandaloneReplicationConfig.getInstance(ReplicationFactor.ONE);
  // STANDALONE with three replicas still reports REDUCED_REDUNDANCY, which only the replication
  // type check catches; the single-replica check does not.
  private static final ReplicationConfig STANDALONE_THREE =
      StandaloneReplicationConfig.getInstance(ReplicationFactor.THREE);
  private static final ReplicationConfig EC_3_2 = new ECReplicationConfig(3, 2);
  private static final ReplicationConfig EC_6_3 = new ECReplicationConfig(6, 3);

  private static final List<ReplicationConfig> REPLICATIONS = Arrays.asList(
      RATIS_ONE, RATIS_THREE, STANDALONE_ONE, STANDALONE_THREE, EC_3_2,
      new ECReplicationConfig(4, 2), EC_6_3,
      new ECReplicationConfig(9, 3));

  /** Every storage policy, plus null for keys written before storage policy support. */
  private static final List<StoragePolicy> POLICIES = Arrays.asList(
      null, OzoneStoragePolicy.HOT, OzoneStoragePolicy.WARM, OzoneStoragePolicy.COLD);

  @Test
  void testFromS3StorageClass() {
    assertEquals(S3StorageClass.REDUCED_REDUNDANCY,
        S3StorageClass.fromS3StorageClass(S3_STORAGE_CLASS_REDUCED_REDUNDANCY));
    assertEquals(S3StorageClass.STANDARD,
        S3StorageClass.fromS3StorageClass(S3_STORAGE_CLASS_STANDARD));
    assertEquals(S3StorageClass.STANDARD_IA,
        S3StorageClass.fromS3StorageClass(S3_STORAGE_CLASS_STANDARD_IA));
    assertEquals(S3StorageClass.GLACIER,
        S3StorageClass.fromS3StorageClass(S3_STORAGE_CLASS_GLACIER));
    assertThrows(IllegalArgumentException.class,
        () -> S3StorageClass.fromS3StorageClass("standard"));
    assertThrows(IllegalArgumentException.class,
        () -> S3StorageClass.fromS3StorageClass("INVALID"));
    assertThrows(IllegalArgumentException.class,
        () -> S3StorageClass.fromS3StorageClass(null));
  }

  @Test
  void testFromS3StorageClassTrimsWhitespace() {
    for (S3StorageClass storageClass : S3StorageClass.values()) {
      String value = storageClass.getS3StorageClass();
      for (String input : Arrays.asList(" " + value, value + " ", "\t " + value + " \t")) {
        assertEquals(storageClass, S3StorageClass.fromS3StorageClass(input));
      }
    }
  }

  @Test
  void testFromS3StorageClassRejectsCaseMismatchAndBlankValues() {
    for (String input : Arrays.asList("Standard", "standard_ia", "glacier", " standard ", "", " \t ", "STANDARD IA")) {
      assertThrows(IllegalArgumentException.class, () -> S3StorageClass.fromS3StorageClass(input));
    }
  }

  /**
   * Keys with no storage policy must report exactly what they reported before storage policy
   * support, so existing objects keep their storage class. Compares against the original
   * replication-only logic, copied here so a change to the production code cannot change both.
   */
  @Test
  void testReverseWithoutPolicyMatchesOriginalMapping() {
    for (ReplicationConfig replication : REPLICATIONS) {
      assertEquals(originalMapping(replication),
          S3StorageClassResolver.toS3StorageClass(replication, null).name(),
          "replication " + replication);
      assertEquals(originalMapping(replication),
          S3StorageType.fromReplicationConfig(replication).name(),
          "deprecated S3StorageType for " + replication);
    }
  }

  /** The reverse mapping describes stored data, so it must give an answer for every combination. */
  @Test
  void testReverseNeverFails() {
    for (ReplicationConfig replication : REPLICATIONS) {
      for (StoragePolicy policy : POLICIES) {
        assertNotNull(S3StorageClassResolver.toS3StorageClass(replication, policy),
            replication + " + " + policy);
      }
    }
    for (StoragePolicy policy : POLICIES) {
      assertEquals(S3StorageClass.STANDARD,
          S3StorageClassResolver.toS3StorageClass(null, policy));
    }
  }

  /** Only COLD changes the class. HOT and WARM leave replication to decide. */
  @Test
  void testReverseOnlyColdChangesTheClass() {
    for (StoragePolicy policy : Arrays.asList(OzoneStoragePolicy.HOT, OzoneStoragePolicy.WARM)) {
      assertEquals(S3StorageClass.STANDARD,
          S3StorageClassResolver.toS3StorageClass(RATIS_THREE, policy));
      assertEquals(S3StorageClass.REDUCED_REDUNDANCY,
          S3StorageClassResolver.toS3StorageClass(RATIS_ONE, policy));
      assertEquals(S3StorageClass.STANDARD_IA,
          S3StorageClassResolver.toS3StorageClass(EC_6_3, policy));
    }

    assertEquals(S3StorageClass.GLACIER,
        S3StorageClassResolver.toS3StorageClass(RATIS_THREE, OzoneStoragePolicy.COLD));
    assertEquals(S3StorageClass.GLACIER,
        S3StorageClassResolver.toS3StorageClass(RATIS_ONE, OzoneStoragePolicy.COLD));
    // There is no EC + COLD class yet, so EC still reports STANDARD_IA.
    assertEquals(S3StorageClass.STANDARD_IA,
        S3StorageClassResolver.toS3StorageClass(EC_6_3, OzoneStoragePolicy.COLD));
  }

  /** Any EC layout reports STANDARD_IA, not just 3-2. */
  @Test
  void testReverseIgnoresEcLayout() {
    for (ReplicationConfig replication : REPLICATIONS) {
      if (replication.getReplicationType() == ReplicationType.EC) {
        assertEquals(S3StorageClass.STANDARD_IA,
            S3StorageClassResolver.toS3StorageClass(replication, OzoneStoragePolicy.WARM));
      }
    }
  }

  @Test
  void testForwardDefaultSpecs() {
    S3StorageClassResolver resolver = new S3StorageClassResolver(new OzoneConfiguration());

    S3StorageClassSpec standard = resolver.toSpec(S3_STORAGE_CLASS_STANDARD);
    assertEquals(ReplicationType.RATIS, standard.getReplicationType());
    assertEquals(ReplicationFactor.THREE, standard.getReplicationFactor());
    assertNull(standard.getStoragePolicy(), "unset by default, so the bucket's policy applies");

    S3StorageClassSpec reduced = resolver.toSpec(S3_STORAGE_CLASS_REDUCED_REDUNDANCY);
    assertEquals(ReplicationFactor.ONE, reduced.getReplicationFactor());
    assertNull(reduced.getStoragePolicy());

    assertEquals(OzoneStoragePolicy.WARM,
        resolver.toSpec(S3_STORAGE_CLASS_STANDARD_IA).getStoragePolicy());
    assertEquals(OzoneStoragePolicy.COLD,
        resolver.toSpec(S3_STORAGE_CLASS_GLACIER).getStoragePolicy());

    for (String input : Arrays.asList(null, "", " \t ", "INVALID", "standard")) {
      assertThrows(IllegalArgumentException.class, () -> resolver.toSpec(input));
    }
  }

  @Test
  void testForwardConfiguredStoragePolicy() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_S3G_STORAGE_CLASS_STANDARD_STORAGE_POLICY, "hot");
    conf.set(OZONE_S3G_STORAGE_CLASS_REDUCED_REDUNDANCY_STORAGE_POLICY, " WARM ");
    S3StorageClassResolver resolver = new S3StorageClassResolver(conf);

    assertEquals(OzoneStoragePolicy.HOT,
        resolver.toSpec(S3_STORAGE_CLASS_STANDARD).getStoragePolicy());
    assertEquals(OzoneStoragePolicy.WARM, resolver.toSpec(S3_STORAGE_CLASS_REDUCED_REDUNDANCY).getStoragePolicy());
    // The setting applies only to its own class.
    assertEquals(OzoneStoragePolicy.COLD,
        resolver.toSpec(S3_STORAGE_CLASS_GLACIER).getStoragePolicy());
  }

  /** COLD would make STANDARD writes report GLACIER, so it is rejected. */
  @Test
  void testConstructionRejectsColdAndUnknownPolicy() {
    for (String key : Arrays.asList(OZONE_S3G_STORAGE_CLASS_STANDARD_STORAGE_POLICY,
        OZONE_S3G_STORAGE_CLASS_REDUCED_REDUNDANCY_STORAGE_POLICY)) {
      for (String value : Arrays.asList("COLD", " cold ", "LUKEWARM")) {
        OzoneConfiguration conf = new OzoneConfiguration();
        conf.set(key, value);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> new S3StorageClassResolver(conf));
        assertTrue(exception.getMessage().contains(key));
        assertTrue(exception.getMessage().contains("Expected HOT, WARM or empty."));
      }
    }
  }

  @Test
  void testForwardReusesPoliciesResolvedAtConstruction() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_S3G_STORAGE_CLASS_STANDARD_STORAGE_POLICY, "HOT");
    conf.set(OZONE_S3G_STORAGE_CLASS_REDUCED_REDUNDANCY_STORAGE_POLICY, "WARM");
    S3StorageClassResolver resolver = new S3StorageClassResolver(conf);
    S3StorageClassSpec standard = resolver.toSpec(S3_STORAGE_CLASS_STANDARD);
    S3StorageClassSpec reduced = resolver.toSpec(S3_STORAGE_CLASS_REDUCED_REDUNDANCY);

    conf.set(OZONE_S3G_STORAGE_CLASS_STANDARD_STORAGE_POLICY, "COLD");
    conf.set(OZONE_S3G_STORAGE_CLASS_REDUCED_REDUNDANCY_STORAGE_POLICY, "INVALID");
    assertSame(standard, resolver.toSpec(S3_STORAGE_CLASS_STANDARD));
    assertSame(reduced, resolver.toSpec(S3_STORAGE_CLASS_REDUCED_REDUNDANCY));
    assertEquals(OzoneStoragePolicy.HOT, standard.getStoragePolicy());
    assertEquals(OzoneStoragePolicy.WARM, reduced.getStoragePolicy());
  }

  /**
   * A STANDARD write with a configured storage policy must read back as STANDARD, so changing the
   * setting never relabels objects.
   */
  @Test
  void testConfiguredPolicyRoundTripsToSameClass() {
    for (String policy : Arrays.asList("", " \t ", "HOT", "WARM")) {
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.set(OZONE_S3G_STORAGE_CLASS_STANDARD_STORAGE_POLICY, policy);
      conf.set(OZONE_S3G_STORAGE_CLASS_REDUCED_REDUNDANCY_STORAGE_POLICY, policy);
      S3StorageClassResolver resolver = new S3StorageClassResolver(conf);
      if (StringUtils.isBlank(policy)) {
        assertNull(resolver.toSpec(S3_STORAGE_CLASS_STANDARD).getStoragePolicy());
        assertNull(resolver.toSpec(S3_STORAGE_CLASS_REDUCED_REDUNDANCY).getStoragePolicy());
      }
      for (S3StorageClass storageClass : S3StorageClass.values()) {
        S3StorageClassSpec spec = resolver.toSpec(storageClass.getS3StorageClass());
        ReplicationConfig replication =
            S3StorageClassResolver.toReplicationConfig(spec, null, null);
        assertEquals(storageClass,
            S3StorageClassResolver.toS3StorageClass(replication, spec.getStoragePolicy()),
            storageClass + " with policy setting '" + policy + "'");
      }
    }
  }

  /** EC layout comes from the request header, then the bucket, then defaults to 3-2. */
  @Test
  void testForwardEcLayoutOrder() {
    S3StorageClassResolver resolver = new S3StorageClassResolver(new OzoneConfiguration());
    S3StorageClassSpec ec = resolver.toSpec(S3_STORAGE_CLASS_STANDARD_IA);

    assertEquals(new ECReplicationConfig("rs-6-3-1024k"),
        S3StorageClassResolver.toReplicationConfig(ec, "rs-6-3-1024k", EC_3_2));
    assertEquals(EC_6_3, S3StorageClassResolver.toReplicationConfig(ec, null, EC_6_3));
    assertEquals(EC_6_3, S3StorageClassResolver.toReplicationConfig(ec, "", EC_6_3));
    // A non-EC bucket replication is not used for an EC class.
    assertEquals(EC_3_2, S3StorageClassResolver.toReplicationConfig(ec, null, RATIS_THREE));
    assertEquals(EC_3_2, S3StorageClassResolver.toReplicationConfig(ec, null, null));

    assertThrows(IllegalArgumentException.class,
        () -> S3StorageClassResolver.toReplicationConfig(ec, "INVALID", EC_6_3));
    assertThrows(IllegalArgumentException.class,
        () -> S3StorageClassResolver.toReplicationConfig(ec, "INVALID", null));
  }

  @Test
  void testForwardRatisReplication() {
    S3StorageClassResolver resolver = new S3StorageClassResolver(new OzoneConfiguration());
    // RATIS classes use their own factor and ignore the bucket's replication.
    assertEquals(RATIS_THREE, S3StorageClassResolver.toReplicationConfig(
        resolver.toSpec(S3_STORAGE_CLASS_STANDARD), null, EC_6_3));
    assertEquals(RATIS_ONE, S3StorageClassResolver.toReplicationConfig(
        resolver.toSpec(S3_STORAGE_CLASS_REDUCED_REDUNDANCY), null, null));
  }

  @Test
  void testForwardRejectsMissingReplicationTypeWithBucketDefault() {
    S3StorageClassSpec spec = S3StorageClassSpec.newBuilder()
        .setStoragePolicy(OzoneStoragePolicy.COLD)
        .build();

    for (ReplicationConfig bucketConfig : Arrays.asList(RATIS_THREE, EC_6_3)) {
      assertThrows(IllegalArgumentException.class,
          () -> S3StorageClassResolver.toReplicationConfig(spec, null, bucketConfig));
    }
  }

  @Test
  void testForwardRejectsMissingReplicationTypeWithoutBucketDefault() {
    S3StorageClassSpec spec = S3StorageClassSpec.newBuilder().build();

    assertThrows(IllegalArgumentException.class,
        () -> S3StorageClassResolver.toReplicationConfig(spec, null, null));
  }

  @Test
  void testSpecIsAValue() {
    S3StorageClassSpec a = S3StorageClassSpec.newBuilder()
        .setReplicationType(ReplicationType.RATIS)
        .setReplicationFactor(ReplicationFactor.THREE)
        .build();
    S3StorageClassSpec b = S3StorageClassSpec.newBuilder()
        .setReplicationType(ReplicationType.RATIS)
        .setReplicationFactor(ReplicationFactor.THREE)
        .build();
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    // Unset fields stay null rather than taking a default.
    assertNull(S3StorageClassSpec.newBuilder().build().getReplicationType());
  }

  /** The replication-only mapping as it was before storage policy support. */
  private static String originalMapping(ReplicationConfig config) {
    if (config.getReplicationType() == ReplicationType.EC) {
      return "STANDARD_IA";
    }
    if (config.getReplicationType() == ReplicationType.STAND_ALONE
        || config.getRequiredNodes() == 1) {
      return "REDUCED_REDUNDANCY";
    }
    return "STANDARD";
  }
}
