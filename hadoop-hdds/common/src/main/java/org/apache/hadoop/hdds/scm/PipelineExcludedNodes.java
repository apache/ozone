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

package org.apache.hadoop.hdds.scm;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

/**
 * Parsed snapshot of {@code hdds.scm.pipeline.exclude.datanodes}: UUIDs, hostnames,
 * and IP tokens to exclude from pipeline creation and selection.
 */
public final class PipelineExcludedNodes {
  public static final PipelineExcludedNodes EMPTY = new PipelineExcludedNodes(
      Collections.emptySet(), Collections.emptySet());

  private final Set<DatanodeID> excludedDatanodeIds;
  private final Set<String> excludedAddressTokens;

  private PipelineExcludedNodes(Set<DatanodeID> excludedDatanodeIds, Set<String> excludedAddressTokens) {
    this.excludedDatanodeIds = ImmutableSet.copyOf(excludedDatanodeIds);
    this.excludedAddressTokens = ImmutableSet.copyOf(excludedAddressTokens);
  }

  public static PipelineExcludedNodes parse(String rawValue) {
    if (rawValue == null || StringUtils.isBlank(rawValue)) {
      return EMPTY;
    }

    Set<DatanodeID> datanodeIDs = new HashSet<>();
    Set<String> addressTokens = new HashSet<>();

    Arrays.stream(rawValue.split(","))
        .map(String::trim)
        .filter(token -> !token.isEmpty())
        .forEach(token -> {
          try {
            datanodeIDs.add(DatanodeID.fromUuidString(token));
          } catch (IllegalArgumentException ignored) {
            addressTokens.add(normalizeAddress(token));
          }
        });

    if (datanodeIDs.isEmpty() && addressTokens.isEmpty()) {
      return EMPTY;
    }
    return new PipelineExcludedNodes(datanodeIDs, addressTokens);
  }

  public boolean isEmpty() {
    return excludedDatanodeIds.isEmpty() && excludedAddressTokens.isEmpty();
  }

  public Set<DatanodeID> getExcludedDatanodeIds() {
    return excludedDatanodeIds;
  }

  public Set<String> getExcludedAddressTokens() {
    return excludedAddressTokens;
  }

  public boolean isExcluded(DatanodeDetails datanodeDetails) {
    if (datanodeDetails == null) {
      return false;
    }
    if (excludedDatanodeIds.contains(datanodeDetails.getID())) {
      return true;
    }

    final String hostName = datanodeDetails.getHostName();
    if (hostName != null && excludedAddressTokens.contains(normalizeAddress(hostName))) {
      return true;
    }

    final String ipAddress = datanodeDetails.getIpAddress();
    return ipAddress != null && excludedAddressTokens.contains(normalizeAddress(ipAddress));
  }

  public boolean isExcluded(Pipeline pipeline) {
    for (DatanodeDetails dn : pipeline.getNodes()) {
      if (isExcluded(dn)) {
        return true;
      }
    }
    return false;
  }

  private static String normalizeAddress(String value) {
    return value.toLowerCase(Locale.ROOT);
  }
}
