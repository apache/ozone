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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.http.PrometheusMetricsSink;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecordImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests attribution, export and lifecycle of committed storage-policy bytes. */
public class TestOMStoragePolicyMetrics {
  @ParameterizedTest
  @EnumSource(OzoneStoragePolicy.class)
  void testPreferredPolicy(OzoneStoragePolicy policy) throws Exception {
    OMStoragePolicyMetrics metrics = new OMStoragePolicyMetrics();
    OmKeyInfo allocated = key(policy, 100, block(1, 100, policy.getCreationTier(), false));
    // Client locations omit the allocation attributes; OM's evidence is authoritative.
    metrics.recordCommit(allocated, key(policy, 100, block(1, 100, null, false)));
    assertThat(export(metrics)).contains("policy=\"" + policy.name() + "\"")
        .contains("requestedtier=\"" + policy.getCreationTier().name() + "\"")
        .contains("actualtier=\"" + policy.getCreationTier().name() + "\"")
        .contains("outcome=\"preferred\"").contains(" 100\n");
  }

  @Test
  void testMixedKeySplitsCommittedBytesAndIgnoresClientClaims() throws Exception {
    OMStoragePolicyMetrics metrics = new OMStoragePolicyMetrics();
    OmKeyInfo allocated = key(OzoneStoragePolicy.HOT, 12,
        block(1, StorageTier.SSD, false), block(2, StorageTier.DISK, true));
    metrics.recordCommit(allocated, key(OzoneStoragePolicy.HOT, 12,
        block(1, 8, StorageTier.ARCHIVE, true), block(2, 4, StorageTier.SSD, false)));
    assertThat(snapshot(metrics)).isEqualTo(Map.of(
        "HOT/SSD/SSD/preferred", 8L, "HOT/SSD/DISK/fallback", 4L));
    String text = export(metrics);
    assertThat(text).contains("# TYPE om_storage_policy_metrics_committed_bytes counter")
        .doesNotContain("MIXED", "bucket=", "key=", "volume=", "user=");
    assertThat(text.lines().filter(line -> !line.startsWith("#")).count()).isEqualTo(2);
    assertThat(allocated.getLatestVersionLocations().getBlocksLatestVersionOnly().get(0).getStorageTier())
        .isEqualTo(StorageTier.SSD);
  }

  @Test
  void testBlocksOnSameTierAggregateWithoutMixingOutcomes() {
    OMStoragePolicyMetrics metrics = new OMStoragePolicyMetrics();
    OmKeyInfo allocated = key(OzoneStoragePolicy.WARM, 18,
        block(1, StorageTier.DISK, false), block(2, StorageTier.DISK, false), block(3, StorageTier.DISK, true));
    metrics.recordCommit(allocated, allocated);
    assertThat(snapshot(metrics)).isEqualTo(Map.of(
        "WARM/DISK/DISK/preferred", 12L, "WARM/DISK/DISK/fallback", 6L));
  }

  @Test
  void testUnusedPreallocationAndEmptyBlockDoNotImplyFallback() throws Exception {
    OMStoragePolicyMetrics metrics = new OMStoragePolicyMetrics();
    OmKeyInfo allocated = key(OzoneStoragePolicy.HOT, 12,
        block(1, StorageTier.SSD, false), block(2, StorageTier.DISK, true));
    OmKeyLocationInfo empty = block(2, null, false);
    empty.setLength(0);
    metrics.recordCommit(allocated, key(OzoneStoragePolicy.HOT, 6, block(1, null, false), empty));
    assertThat(export(metrics)).contains("actualtier=\"SSD\"").contains("outcome=\"preferred\"");
  }

  @Test
  void testLegacyAndIncompleteEvidenceIsUnknown() throws Exception {
    OMStoragePolicyMetrics metrics = new OMStoragePolicyMetrics();
    metrics.recordCommit(null, key(null, 10, block(1, StorageTier.DISK, false)));
    metrics.recordCommit(key(OzoneStoragePolicy.HOT, 20, block(1, null, false)),
        key(OzoneStoragePolicy.HOT, 20, block(1, StorageTier.SSD, false)));
    String text = export(metrics);
    assertThat(text).contains("policy=\"UNKNOWN\",requestedtier=\"UNKNOWN\",actualtier=\"UNKNOWN\"")
        .contains("policy=\"HOT\",requestedtier=\"SSD\",actualtier=\"UNKNOWN\"")
        .doesNotContain("outcome=\"preferred\"");
  }

  @Test
  void testKnownFallbackAndUnknownBlocksAreCountedSeparately() {
    OMStoragePolicyMetrics metrics = new OMStoragePolicyMetrics();
    metrics.recordCommit(key(OzoneStoragePolicy.HOT, 12, block(1, StorageTier.DISK, true)),
        key(OzoneStoragePolicy.HOT, 12, block(1, null, false), block(2, null, false)));
    assertThat(snapshot(metrics)).isEqualTo(Map.of(
        "HOT/SSD/DISK/fallback", 6L, "HOT/SSD/UNKNOWN/unknown", 6L));
  }

  @Test
  void testFallbackWithUnknownTierRetainsKnownOutcome() {
    OMStoragePolicyMetrics metrics = new OMStoragePolicyMetrics();
    OmKeyInfo key = key(OzoneStoragePolicy.HOT, 6, block(1, null, true));
    metrics.recordCommit(key, key);
    assertThat(snapshot(metrics)).isEqualTo(Map.of("HOT/SSD/UNKNOWN/fallback", 6L));
  }

  @Test
  void testUnaccountedBytesAreUnknown() {
    OMStoragePolicyMetrics metrics = new OMStoragePolicyMetrics();
    OmKeyInfo key = key(OzoneStoragePolicy.HOT, 10, block(1, StorageTier.SSD, false));
    metrics.recordCommit(key, key);
    assertThat(snapshot(metrics)).isEqualTo(Map.of(
        "HOT/SSD/SSD/preferred", 6L, "HOT/SSD/UNKNOWN/unknown", 4L));
  }

  @ParameterizedTest
  @ValueSource(longs = {-1, 7, Long.MAX_VALUE})
  void testInvalidBlockLengthsDoNotOvercountOrPartiallyPublish(long invalidLength) {
    OMStoragePolicyMetrics metrics = new OMStoragePolicyMetrics();
    // First block is valid, but neither it nor the invalid block may leak into the counters.
    OmKeyInfo key = key(OzoneStoragePolicy.HOT, 12,
        block(1, StorageTier.SSD, false), block(2, invalidLength, StorageTier.DISK, true));
    metrics.recordCommit(key, key);
    assertThat(snapshot(metrics)).isEqualTo(Map.of("HOT/SSD/UNKNOWN/unknown", 12L));
  }

  @Test
  void testLargestValidLogicalSizeDoesNotOverflow() {
    OMStoragePolicyMetrics metrics = new OMStoragePolicyMetrics();
    OmKeyInfo key = key(OzoneStoragePolicy.HOT, Long.MAX_VALUE,
        block(1, Long.MAX_VALUE - 1, StorageTier.SSD, false), block(2, 1, StorageTier.DISK, true));
    metrics.recordCommit(key, key);
    assertThat(snapshot(metrics)).isEqualTo(Map.of(
        "HOT/SSD/SSD/preferred", Long.MAX_VALUE - 1, "HOT/SSD/DISK/fallback", 1L));
  }

  @Test
  void testECCountsLogicalBytesWithoutReplicationOverhead() {
    OMStoragePolicyMetrics metrics = new OMStoragePolicyMetrics();
    OmKeyInfo key = key(OzoneStoragePolicy.HOT, 12,
        block(1, 8, StorageTier.SSD, false), block(2, 4, StorageTier.DISK, true))
        .toBuilder().setReplicationConfig(new ECReplicationConfig(3, 2)).build();
    metrics.recordCommit(key, key);
    assertThat(snapshot(metrics)).isEqualTo(Map.of(
        "HOT/SSD/SSD/preferred", 8L, "HOT/SSD/DISK/fallback", 4L));
  }

  @Test
  void testInconsistentTierWithoutFallbackEvidenceIsUnknown() throws Exception {
    OMStoragePolicyMetrics metrics = new OMStoragePolicyMetrics();
    OmKeyInfo key = key(OzoneStoragePolicy.HOT, 6, block(1, StorageTier.DISK, false));
    metrics.recordCommit(key, key);
    assertThat(export(metrics)).contains("actualtier=\"DISK\"").contains("outcome=\"unknown\"");
  }

  @Test
  void testEmptyAndMissingLocations() throws Exception {
    OMStoragePolicyMetrics metrics = new OMStoragePolicyMetrics();
    metrics.recordCommit(null, null);
    metrics.recordCommit(null, key(OzoneStoragePolicy.HOT, 0));
    assertThat(export(metrics)).isEmpty();
    metrics.recordCommit(null, key(OzoneStoragePolicy.HOT, 10));
    assertThat(export(metrics)).contains("actualtier=\"UNKNOWN\"").contains("outcome=\"unknown\"");
  }

  @Test
  void testHistoricalVersionsDoNotChangeCurrentKeyAttribution() throws Exception {
    OMStoragePolicyMetrics metrics = new OMStoragePolicyMetrics();
    OmKeyLocationInfo old = block(1, StorageTier.DISK, true);
    OmKeyLocationInfo current = block(2, 10, StorageTier.SSD, false);
    current.setCreateVersion(1);
    OmKeyInfo allocated = key(OzoneStoragePolicy.HOT, 10).toBuilder().setOmKeyLocationInfos(Arrays.asList(
        new OmKeyLocationInfoGroup(0, Collections.singletonList(old)),
        new OmKeyLocationInfoGroup(1, Arrays.asList(old, current)))).build();
    OmKeyInfo committed = allocated.toBuilder().build();
    metrics.recordCommit(allocated, committed);
    assertThat(export(metrics)).contains("actualtier=\"SSD\"").contains("outcome=\"preferred\"")
        .contains(" 10\n");
  }

  @Test
  void testOMMetricsOwnsRegistrationAndCleanup() {
    OMMetrics owner = OMMetrics.create(null);
    try {
      assertThat(DefaultMetricsSystem.instance().getSource("OMStoragePolicyMetrics"))
          .isInstanceOf(OMStoragePolicyMetrics.class);
    } finally {
      owner.unRegister();
    }
    assertNull(DefaultMetricsSystem.instance().getSource("OMStoragePolicyMetrics"));
  }

  @Test
  void testConcurrentCommitsAndScrapes() throws Exception {
    OMStoragePolicyMetrics metrics = new OMStoragePolicyMetrics();
    OmKeyInfo key = key(OzoneStoragePolicy.HOT, 12,
        block(1, 8, StorageTier.SSD, false), block(2, 4, StorageTier.DISK, true));
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      List<Future<?>> futures = IntStream.range(0, 4).mapToObj(i -> executor.submit(() -> {
        for (int j = 0; j < 100; j++) {
          metrics.recordCommit(key, key);
          metrics.getMetrics(new MetricsCollectorImpl(), true);
        }
      })).collect(Collectors.toList());
      for (Future<?> future : futures) {
        future.get();
      }
    } finally {
      executor.shutdownNow();
    }
    assertThat(snapshot(metrics)).isEqualTo(Map.of(
        "HOT/SSD/SSD/preferred", 3200L, "HOT/SSD/DISK/fallback", 1600L));
  }

  @Test
  void testSourceLifecycleAndReset() throws Exception {
    boolean miniClusterMode = DefaultMetricsSystem.inMiniClusterMode();
    DefaultMetricsSystem.setMiniClusterMode(true);
    OzoneConfiguration firstConf = new OzoneConfiguration();
    firstConf.set(OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID, "service");
    firstConf.set(OMConfigKeys.OZONE_OM_NODE_ID_KEY, "one");
    OzoneConfiguration secondConf = new OzoneConfiguration(firstConf);
    secondConf.set(OMConfigKeys.OZONE_OM_NODE_ID_KEY, "two");
    OMStoragePolicyMetrics first = new OMStoragePolicyMetrics();
    OMStoragePolicyMetrics second = new OMStoragePolicyMetrics();
    try {
      first.register(firstConf);
      second.register(secondConf);
      assertSame(first, DefaultMetricsSystem.instance().getSource("OMStoragePolicyMetrics-service-one"));
      assertSame(second, DefaultMetricsSystem.instance().getSource("OMStoragePolicyMetrics-service-two"));
      first.recordCommit(null, key(null, 10));
      first.unregister();
      assertNull(DefaultMetricsSystem.instance().getSource("OMStoragePolicyMetrics-service-one"));
      assertSame(second, DefaultMetricsSystem.instance().getSource("OMStoragePolicyMetrics-service-two"));
      first = new OMStoragePolicyMetrics();
      first.register(firstConf);
      assertThat(export(first)).isEmpty();
    } finally {
      first.unregister();
      second.unregister();
      DefaultMetricsSystem.setMiniClusterMode(miniClusterMode);
    }
  }

  private static OmKeyLocationInfo block(long localID, StorageTier tier, boolean fallback) {
    return block(localID, 6, tier, fallback);
  }

  private static OmKeyLocationInfo block(long localID, long length, StorageTier tier, boolean fallback) {
    return new OmKeyLocationInfo.Builder().setBlockID(new BlockID(1, localID)).setLength(length)
        .setStorageTier(tier).setIsFallBack(fallback).build();
  }

  private static Map<String, Long> snapshot(OMStoragePolicyMetrics metrics) {
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    metrics.getMetrics(collector, true);
    Map<String, Long> result = new HashMap<>();
    for (MetricsRecordImpl record : collector.getRecords()) {
      Map<String, String> tags = record.tags().stream()
          .collect(Collectors.toMap(tag -> tag.name(), tag -> tag.value()));
      for (AbstractMetric metric : record.metrics()) {
        result.put(tags.get("Policy") + "/" + tags.get("RequestedTier") + "/" + tags.get("ActualTier") + "/"
            + tags.get("Outcome"), metric.value().longValue());
      }
    }
    return result;
  }

  private static OmKeyInfo key(OzoneStoragePolicy policy, long size, OmKeyLocationInfo... blocks) {
    return new OmKeyInfo.Builder().setVolumeName("volume").setBucketName("bucket").setKeyName("key")
        .setReplicationConfig(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE))
        .setDataSize(size).setStoragePolicy(policy)
        .setOmKeyLocationInfos(Collections.singletonList(new OmKeyLocationInfoGroup(0, Arrays.asList(blocks))))
        .build();
  }

  private static String export(OMStoragePolicyMetrics metrics) throws Exception {
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    metrics.getMetrics(collector, true);
    PrometheusMetricsSink sink = new PrometheusMetricsSink("om");
    collector.getRecords().forEach(sink::putMetrics);
    sink.flush();
    StringWriter writer = new StringWriter();
    sink.writeMetrics(writer);
    return writer.toString();
  }
}
