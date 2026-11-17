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

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.util.MetricUtil;

/**
 * Committed logical bytes attributed to each block's storage-policy allocation outcome. Bytes from a key
 * spanning multiple tiers are counted separately per tier. These are per-OM counters with the same commit
 * boundary as {@code OMMetrics.TotalDataCommitted}, not current physical-volume usage.
 */
public final class OMStoragePolicyMetrics implements MetricsSource {
  private static final String SOURCE_NAME = OMStoragePolicyMetrics.class.getSimpleName();
  private static final MetricsInfo POLICY = Interns.info("Policy", "Recorded key storage policy");
  private static final MetricsInfo REQUESTED_TIER = Interns.info("RequestedTier", "Policy creation tier");
  private static final MetricsInfo ACTUAL_TIER = Interns.info("ActualTier", "SCM-selected tiers of committed blocks");
  private static final MetricsInfo OUTCOME = Interns.info("Outcome", "Preferred, fallback or unknown allocation");
  private static final MetricsInfo COMMITTED_BYTES = Interns.info("CommittedBytes", "Committed logical key bytes");

  private final ConcurrentMap<Triple<OzoneStoragePolicy, Tier, Outcome>, AtomicLong> committedBytes =
      new ConcurrentHashMap<>();
  private String sourceName;

  // These values bound metric cardinality; key, bucket and volume names are never retained or exported.
  private enum Tier {
    SSD, DISK, ARCHIVE, UNKNOWN
  }

  private enum Outcome {
    PREFERRED, FALLBACK, UNKNOWN
  }

  void register(ConfigurationSource conf) {
    sourceName = MetricUtil.getMetricsSourceName(SOURCE_NAME,
        conf == null ? null : conf.get(OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID),
        conf == null ? null : conf.get(OMConfigKeys.OZONE_OM_NODE_ID_KEY));
    DefaultMetricsSystem.instance().register(sourceName, "OM storage policy commit metrics", this);
  }

  void unregister() {
    if (sourceName != null) {
      DefaultMetricsSystem.instance().unregisterSource(sourceName);
    }
  }

  /**
   * Counts one successful final commit using OM-owned allocation metadata and the accepted committed block IDs.
   * Client commit locations can omit or disagree with allocation attributes. Neither key is modified or retained.
   * Committed block lengths split the key size across series; unaccounted bytes are UNKNOWN. Invalid lengths
   * make the entire key UNKNOWN so counters cannot exceed its size. Empty keys add no bytes. Callers must exclude
   * failures and intermediate hsync commits.
   */
  void recordCommit(OmKeyInfo allocatedKey, OmKeyInfo committedKey) {
    if (committedKey == null || committedKey.getDataSize() <= 0) {
      return;
    }
    OzoneStoragePolicy policy = committedKey.getStoragePolicy() instanceof OzoneStoragePolicy
        ? (OzoneStoragePolicy) committedKey.getStoragePolicy() : null;
    Tier requestedTier = tier(policy == null ? null : policy.getCreationTier());
    Map<ContainerBlockID, OmKeyLocationInfo> allocations = new HashMap<>();
    OmKeyLocationInfoGroup allocatedLocations = allocatedKey == null ? null : allocatedKey.getLatestVersionLocations();
    if (allocatedLocations != null) {
      for (List<OmKeyLocationInfo> locations : allocatedLocations.getLocationLists()) {
        for (OmKeyLocationInfo location : locations) {
          allocations.put(location.getBlockID().getContainerBlockID(), location);
        }
      }
    }

    Map<Triple<OzoneStoragePolicy, Tier, Outcome>, Long> blockBytes = new HashMap<>();
    long remaining = committedKey.getDataSize();
    OmKeyLocationInfoGroup committedLocations = committedKey.getLatestVersionLocations();
    if (committedLocations != null) {
      for (OmKeyLocationInfo committed : committedLocations.getBlocksLatestVersionOnly()) {
        long length = committed.getLength();
        if (length < 0 || length > remaining) {
          // Inconsistent lengths cannot support an accurate split. Preserve the total without guessing a tier.
          addBytes(Triple.of(policy, Tier.UNKNOWN, Outcome.UNKNOWN), committedKey.getDataSize());
          return;
        }
        if (length == 0) {
          continue;
        }
        remaining -= length;
        OmKeyLocationInfo allocated = allocations.get(committed.getBlockID().getContainerBlockID());
        Tier selected = tier(allocated == null ? null : allocated.getStorageTier());
        Outcome outcome = allocated != null && allocated.getIsFallBack() ? Outcome.FALLBACK
            : requestedTier != Tier.UNKNOWN && selected == requestedTier ? Outcome.PREFERRED : Outcome.UNKNOWN;
        blockBytes.merge(Triple.of(policy, selected, outcome), length, Long::sum);
      }
    }
    if (remaining > 0) {
      blockBytes.merge(Triple.of(policy, Tier.UNKNOWN, Outcome.UNKNOWN), remaining, Long::sum);
    }
    blockBytes.forEach(this::addBytes);
  }

  private void addBytes(Triple<OzoneStoragePolicy, Tier, Outcome> attribution, long bytes) {
    committedBytes.computeIfAbsent(attribution, ignored -> new AtomicLong()).addAndGet(bytes);
  }

  private static Tier tier(StorageTier storageTier) {
    if (storageTier == null) {
      return Tier.UNKNOWN;
    }
    switch (storageTier) {
    case SSD:
      return Tier.SSD;
    case DISK:
      return Tier.DISK;
    case ARCHIVE:
      return Tier.ARCHIVE;
    default:
      return Tier.UNKNOWN;
    }
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    committedBytes.forEach((attribution, bytes) -> {
      OzoneStoragePolicy policy = attribution.getLeft();
      collector.addRecord(SOURCE_NAME).setContext("dfs")
          .tag(POLICY, policy == null ? "UNKNOWN" : policy.name())
          .tag(REQUESTED_TIER, tier(policy == null ? null : policy.getCreationTier()).name())
          .tag(ACTUAL_TIER, attribution.getMiddle().name())
          .tag(OUTCOME, attribution.getRight().name().toLowerCase(Locale.ROOT))
          .addCounter(COMMITTED_BYTES, bytes.get());
    });
  }
}
