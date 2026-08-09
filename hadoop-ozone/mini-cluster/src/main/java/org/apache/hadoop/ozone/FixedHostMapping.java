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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;

/**
 * A {@link CachedDNSToSwitchMapping} implementation that resolves hostnames
 * to rack locations using a statically configured map, bypassing DNS lookups.
 *
 * <p>This is intended for use in test environments (e.g. {@code MiniOzoneCluster})
 * where DataNode hostnames may be synthetic or unresolvable via DNS. The standard
 * {@link CachedDNSToSwitchMapping} performs DNS normalization before rack resolution,
 * which can cause synthetic hostnames to be incorrectly resolved to a real IP address,
 * leading to rack mapping failures. This class avoids that by resolving directly
 * against the registered hostname.
 *
 * <p>The mapping is stored in a JVM-wide static map. Callers must invoke
 * {@link #addNode(String, String)} before cluster startup to register hostname-to-rack
 * entries, and should call {@link #clear()} after each test to avoid cross-test pollution.
 *
 * <p>Usage:
 * <pre>{@code
 * FixedHostMapping.addNode("dn-0.test", "/rack1");
 * FixedHostMapping.addNode("dn-1.test", "/rack1");
 * FixedHostMapping.addNode("dn-2.test", "/rack2");
 *
 * conf.setClass(
 *     CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
 *     FixedHostMapping.class,
 *     DNSToSwitchMapping.class);
 * }</pre>
 */
public class FixedHostMapping extends CachedDNSToSwitchMapping {

  private static final Map<String, String> RACK_MAP = new ConcurrentHashMap<>();

  /**
   * Constructs a {@code FixedHostMapping} with a no-op raw mapping.
   * The raw mapping is unused since {@link #resolve(List)} is fully overridden.
   */
  public FixedHostMapping() {
    super(new NoOpMapping());
  }

  /**
   * Constructs a {@code FixedHostMapping} with the given raw mapping.
   * The raw mapping is unused since {@link #resolve(List)} is fully overridden,
   * but is accepted to satisfy {@link CachedDNSToSwitchMapping} constructor requirements.
   *
   * @param rawMapping the raw DNS mapping (not used for resolution)
   */
  public FixedHostMapping(DNSToSwitchMapping rawMapping) {
    super(rawMapping);
  }

  /**
   * Registers a hostname-to-rack mapping entry.
   * Must be called before cluster startup for the mapping to take effect during
   * DataNode registration.
   *
   * @param host the DataNode hostname as it will appear in {@link #resolve(List)}
   * @param rack the rack path (e.g. {@code "/rack1"})
   */
  public static void addNode(String host, String rack) {
    RACK_MAP.put(host, rack);
  }

  /**
   * Clears all registered hostname-to-rack mappings.
   * Should be called in test teardown (e.g. {@code @AfterEach}) to prevent
   * cross-test pollution of the JVM-wide static map.
   */
  public static void clear() {
    RACK_MAP.clear();
  }

  /**
   * Resolves a list of hostnames to their rack locations using the static map.
   * Hostnames not present in the map are assigned {@link NetworkTopology#DEFAULT_RACK}.
   * Unlike the parent class, this method does not perform DNS normalization.
   *
   * @param names the list of hostnames to resolve
   * @return a list of rack paths in the same order as the input
   */
  @Override
  public List<String> resolve(List<String> names) {
    return names.stream()
        .map(name -> RACK_MAP.getOrDefault(name, NetworkTopology.DEFAULT_RACK))
        .collect(Collectors.toList());
  }

  /**
   * No-op: this implementation does not maintain a cache.
   */
  @Override
  public void reloadCachedMappings() {
  }

  /**
   * No-op: this implementation does not maintain a cache.
   *
   * @param names the hostnames whose cached mappings should be reloaded (ignored)
   */
  @Override
  public void reloadCachedMappings(List<String> names) {
  }

  /**
   * A no-op {@link DNSToSwitchMapping} used as a placeholder raw mapping.
   * All hostnames are mapped to {@link NetworkTopology#DEFAULT_RACK}.
   * This is never invoked during normal resolution since {@link FixedHostMapping#resolve(List)}
   * is fully overridden.
   */
  private static class NoOpMapping implements DNSToSwitchMapping {

    @Override
    public List<String> resolve(List<String> names) {
      return names.stream()
          .map(n -> NetworkTopology.DEFAULT_RACK)
          .collect(Collectors.toList());
    }

    @Override
    public void reloadCachedMappings() {
    }

    @Override
    public void reloadCachedMappings(List<String> names) {
    }
  }
}
