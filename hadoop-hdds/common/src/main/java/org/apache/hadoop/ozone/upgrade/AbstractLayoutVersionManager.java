/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.upgrade;

import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.ALREADY_FINALIZED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_REQUIRED;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.apache.hadoop.metrics2.util.MBeans;

/**
 * Layout Version Manager containing generic method implementations.
 */
@SuppressWarnings("visibilitymodifier")
public abstract class AbstractLayoutVersionManager<T extends LayoutFeature>
    implements LayoutVersionManager, LayoutVersionManagerMXBean {

  protected int metadataLayoutVersion; // MLV.
  protected int softwareLayoutVersion; // SLV.
  protected TreeMap<Integer, T> features = new TreeMap<>();
  protected Map<String, T> featureMap = new HashMap<>();
  protected volatile boolean isInitialized = false;
  protected volatile UpgradeFinalizer.Status currentUpgradeState =
      FINALIZATION_REQUIRED;

  protected void init(int version, T[] lfs) throws IOException {

    if (!isInitialized) {
      metadataLayoutVersion = version;
      initializeFeatures(lfs);
      softwareLayoutVersion = features.lastKey();
      isInitialized = true;
      if (softwareIsBehindMetaData()) {
        throw new IOException(
            String.format("Cannot initialize VersionManager. Metadata " +
                    "layout version (%d) > software layout version (%d)",
                metadataLayoutVersion, softwareLayoutVersion));
      } else if (metadataLayoutVersion == softwareLayoutVersion) {
        currentUpgradeState = ALREADY_FINALIZED;
      }
    }

    MBeans.register("LayoutVersionManager",
        "AbstractLayoutVersionManager", this);
  }

  public UpgradeFinalizer.Status getUpgradeState() {
    return currentUpgradeState;
  }

  private void initializeFeatures(T[] lfs) {
    Arrays.stream(lfs).forEach(f -> {
      Preconditions.checkArgument(!featureMap.containsKey(f.name()));
      Preconditions.checkArgument(!features.containsKey(f.layoutVersion()));
      features.put(f.layoutVersion(), f);
      featureMap.put(f.name(), f);
    });
  }

  protected void reset() {
    metadataLayoutVersion = 0;
    softwareLayoutVersion = 0;
    featureMap.clear();
    features.clear();
    isInitialized = false;
    currentUpgradeState = ALREADY_FINALIZED;
  }

  public void finalized(T layoutFeature) {
    if (layoutFeature.layoutVersion() == metadataLayoutVersion + 1) {
      metadataLayoutVersion = layoutFeature.layoutVersion();
    } else {
      String msgStart = "";
      if (layoutFeature.layoutVersion() < metadataLayoutVersion) {
        msgStart = "Finalize attempt on a layoutFeature which has already "
            + "been finalized.";
      } else {
        msgStart = "Finalize attempt on a layoutFeature that is newer than the"
            + " next feature to be finalized.";
      }

      throw new IllegalArgumentException(
          msgStart + "Software Layout version: " + softwareLayoutVersion
              + " Feature Layout version: " + layoutFeature.layoutVersion());
    }
  }

  public void completeFinalization() {
    currentUpgradeState = FINALIZATION_DONE;
  }

  private boolean softwareIsBehindMetaData() {
    return metadataLayoutVersion > softwareLayoutVersion;
  }

  @Override
  public int getMetadataLayoutVersion() {
    return metadataLayoutVersion;
  }

  @Override
  public int getSoftwareLayoutVersion() {
    return softwareLayoutVersion;
  }

  @Override
  public boolean needsFinalization() {
    return metadataLayoutVersion < softwareLayoutVersion;
  }

  @Override
  public boolean isAllowed(LayoutFeature layoutFeature) {
    return layoutFeature.layoutVersion() <= metadataLayoutVersion;
  }

  @Override
  public boolean isAllowed(String featureName) {
    return featureMap.containsKey(featureName) &&
        isAllowed(featureMap.get(featureName));
  }

  @Override
  public T getFeature(String name) {
    return featureMap.get(name);
  }

  @Override
  public Iterable<T> unfinalizedFeatures() {
    return features
        .tailMap(metadataLayoutVersion + 1)
        .values()
        .stream()
        .collect(Collectors.toList());
  }
}
