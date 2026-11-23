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

  private static final String UNKNOWN = "UNKNOWN";

  // Keyed by policy, the SCM-selected StorageTier (null when unknown) and the outcome. These bound metric
  // cardinality; key, bucket and volume names are never retained or exported.
  private final ConcurrentMap<Triple<OzoneStoragePolicy, StorageTier, Outcome>, AtomicLong> committedBytes =
      new ConcurrentHashMap<>();
  private String sourceName;

  private enum Outcome {
    PREFERRED, FALLBACK, UNKNOWN
  }

  /** Registers this source once; repeated calls on the same instance are no-ops. */
  synchronized void register(ConfigurationSource conf) {
    if (sourceName != null) {
      return;
    }
    String name = MetricUtil.getMetricsSourceName(SOURCE_NAME,
        conf == null ? null : conf.get(OMConfigKeys.OZONE_OM_INTERNAL_SERVICE_ID),
        conf == null ? null : conf.get(OMConfigKeys.OZONE_OM_NODE_ID_KEY));
    DefaultMetricsSystem.instance().register(name, "OM storage policy commit metrics", this);
    sourceName = name;
  }

  /** Unregisters this source if registered, so a later {@link #register} can register it again. */
  synchronized void unregister() {
    if (sourceName != null) {
      DefaultMetricsSystem.instance().unregisterSource(sourceName);
      sourceName = null;
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
    StorageTier requestedTier = knownTier(policy == null ? null : policy.getCreationTier());
    Map<ContainerBlockID, OmKeyLocationInfo> allocations = new HashMap<>();
    OmKeyLocationInfoGroup allocatedLocations = allocatedKey == null ? null : allocatedKey.getLatestVersionLocations();
    if (allocatedLocations != null) {
      for (List<OmKeyLocationInfo> locations : allocatedLocations.getLocationLists()) {
        for (OmKeyLocationInfo location : locations) {
          allocations.put(location.getBlockID().getContainerBlockID(), location);
        }
      }
    }

    Map<Triple<OzoneStoragePolicy, StorageTier, Outcome>, Long> blockBytes = new HashMap<>();
    long remaining = committedKey.getDataSize();
    OmKeyLocationInfoGroup committedLocations = committedKey.getLatestVersionLocations();
    if (committedLocations != null) {
      for (OmKeyLocationInfo committed : committedLocations.getBlocksLatestVersionOnly()) {
        long length = committed.getLength();
        if (length < 0 || length > remaining) {
          // Inconsistent lengths cannot support an accurate split. Preserve the total without guessing a tier.
          addBytes(Triple.of(policy, null, Outcome.UNKNOWN), committedKey.getDataSize());
          return;
        }
        if (length == 0) {
          continue;
        }
        remaining -= length;
        OmKeyLocationInfo allocated = allocations.get(committed.getBlockID().getContainerBlockID());
        StorageTier selected = knownTier(allocated == null ? null : allocated.getStorageTier());
        blockBytes.merge(Triple.of(policy, selected, outcome(allocated, requestedTier, selected)), length, Long::sum);
      }
    }
    if (remaining > 0) {
      blockBytes.merge(Triple.of(policy, null, Outcome.UNKNOWN), remaining, Long::sum);
    }
    blockBytes.forEach(this::addBytes);
  }

  private void addBytes(Triple<OzoneStoragePolicy, StorageTier, Outcome> attribution, long bytes) {
    committedBytes.computeIfAbsent(attribution, ignored -> new AtomicLong()).addAndGet(bytes);
  }

  /** FALLBACK if OM allocated the block as fallback; PREFERRED if its tier is the requested tier; else UNKNOWN. */
  private static Outcome outcome(OmKeyLocationInfo allocated, StorageTier requested, StorageTier selected) {
    if (allocated != null && allocated.getIsFallBack()) {
      // Kept even when the tier is unknown.
      return Outcome.FALLBACK;
    }
    if (requested != null && selected == requested) {
      return Outcome.PREFERRED;
    }
    // No allocation record, or a tier mismatch without a fallback flag.
    return Outcome.UNKNOWN;
  }

  /**
   * Returns the tier when it names real storage, or null when it is unknown. EMPTY only marks "no fallback tier"
   * on a policy and is never a block's tier, so it is treated as unknown.
   */
  private static StorageTier knownTier(StorageTier storageTier) {
    return storageTier == StorageTier.EMPTY ? null : storageTier;
  }

  /** The exported tier label: the StorageTier name, or UNKNOWN. A new StorageTier is reported under its own name. */
  static String tierLabel(StorageTier storageTier) {
    StorageTier tier = knownTier(storageTier);
    return tier == null ? UNKNOWN : tier.name();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    committedBytes.forEach((attribution, bytes) -> {
      OzoneStoragePolicy policy = attribution.getLeft();
      collector.addRecord(SOURCE_NAME).setContext("dfs")
          .tag(POLICY, policy == null ? UNKNOWN : policy.name())
          .tag(REQUESTED_TIER, tierLabel(policy == null ? null : policy.getCreationTier()))
          .tag(ACTUAL_TIER, tierLabel(attribution.getMiddle()))
          .tag(OUTCOME, attribution.getRight().name().toLowerCase(Locale.ROOT))
          .addCounter(COMMITTED_BYTES, bytes.get());
    });
  }
}
