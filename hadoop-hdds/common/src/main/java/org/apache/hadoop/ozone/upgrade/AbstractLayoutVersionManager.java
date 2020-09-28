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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import com.google.common.base.Preconditions;

/**
 * Layout Version Manager containing generic method implementations.
 */
@SuppressWarnings("visibilitymodifier")
public abstract class AbstractLayoutVersionManager implements
    LayoutVersionManager {

  protected int metadataLayoutVersion; // MLV.
  protected int softwareLayoutVersion; // SLV.
  protected TreeMap<Integer, LayoutFeature> features = new TreeMap<>();
  protected Map<String, LayoutFeature> featureMap = new HashMap<>();
  protected volatile boolean isInitialized = false;

  protected void init(int version, LayoutFeature[] lfs) throws IOException {
    if (!isInitialized) {
      metadataLayoutVersion = version;
      initializeFeatures(lfs);
      softwareLayoutVersion = features.lastKey();
      isInitialized = true;
      if (metadataLayoutVersion > softwareLayoutVersion) {
        throw new IOException(
            String.format("Cannot initialize VersionManager. Metadata " +
                    "layout version (%d) > software layout version (%d)",
                metadataLayoutVersion, softwareLayoutVersion));
      }
    }
  }

  protected void initializeFeatures(LayoutFeature[] lfs) {
    Arrays.stream(lfs).forEach(f -> {
      Preconditions.checkArgument(!featureMap.containsKey(f.name()));
      Preconditions.checkArgument(!features.containsKey(f.layoutVersion()));
      features.put(f.layoutVersion(), f);
      featureMap.put(f.name(), f);
    });
  }

  public int getMetadataLayoutVersion() {
    return metadataLayoutVersion;
  }

  public int getSoftwareLayoutVersion() {
    return softwareLayoutVersion;
  }

  public boolean needsFinalization() {
    return metadataLayoutVersion < softwareLayoutVersion;
  }

  public boolean isAllowed(LayoutFeature layoutFeature) {
    return layoutFeature.layoutVersion() <= metadataLayoutVersion;
  }

  public boolean isAllowed(String featureName) {
    return featureMap.containsKey(featureName) &&
        isAllowed(featureMap.get(featureName));
  }

  public LayoutFeature getFeature(String name) {
    return featureMap.get(name);
  }

  public void doFinalize(Object param) {
    if (needsFinalization()){
      Iterator<Map.Entry<Integer, LayoutFeature>> iterator = features
          .tailMap(metadataLayoutVersion + 1).entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Integer, LayoutFeature> f = iterator.next();
        Optional<? extends LayoutFeature.UpgradeAction> upgradeAction =
            f.getValue().onFinalizeAction();
        upgradeAction.ifPresent(action -> action.executeAction(param));
        // ToDo : Handle shutdown while iterating case (resume from last
        //  feature).
        metadataLayoutVersion = f.getKey();
      }
      // ToDo : Persist new MLV.
    }
  }

  protected void reset() {
    metadataLayoutVersion = 0;
    softwareLayoutVersion = 0;
    featureMap.clear();
    features.clear();
    isInitialized = false;
  }
}
