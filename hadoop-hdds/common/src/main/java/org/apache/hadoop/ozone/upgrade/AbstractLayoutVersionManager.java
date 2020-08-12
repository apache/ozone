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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.base.Preconditions;

/**
 * Layout Version Manager containing generic method implementations.
 */
@SuppressWarnings("visibilitymodifier")
public abstract class AbstractLayoutVersionManager {

  protected static int metadataLayoutVersion; // MLV.
  protected static int softwareLayoutVersion; // SLV.
  protected static TreeMap<Integer, Set<LayoutFeature>> features =
      new TreeMap<>();
  protected static Map<String, LayoutFeature> featureMap = new HashMap<>();
  protected static volatile boolean isInitialized = false;

  protected static void init(int version, LayoutFeature[] lfs) {
    if (!isInitialized) {
      metadataLayoutVersion = version;
      initializeFeatures(lfs);
      softwareLayoutVersion = features.lastKey();
      isInitialized = true;
    }
  }

  protected static void initializeFeatures(LayoutFeature[] lfs) {
    Arrays.stream(lfs).forEach(f -> {
      Preconditions.checkArgument(!featureMap.containsKey(f.name()));
      features.computeIfAbsent(f.layoutVersion(), v -> new HashSet<>());
      features.get(f.layoutVersion()).add(f);
      featureMap.put(f.name(), f);
    });
  }

  public static int getMetadataLayoutVersion() {
    return metadataLayoutVersion;
  }

  public static int getSoftwareLayoutVersion() {
    return softwareLayoutVersion;
  }

  public static boolean needsFinalization() {
    return metadataLayoutVersion < softwareLayoutVersion;
  }

  public static boolean isAllowed(LayoutFeature layoutFeature) {
    return layoutFeature.layoutVersion() <= metadataLayoutVersion;
  }

  public static boolean isAllowed(String featureName) {
    return featureMap.containsKey(featureName) &&
        isAllowed(featureMap.get(featureName));
  }

  public static LayoutFeature getFeature(String name) {
    return featureMap.get(name);
  }

  public synchronized static void doFinalize(Object param) {
    if (needsFinalization()){
      Iterator<Map.Entry<Integer, Set<LayoutFeature>>> iterator = features
          .tailMap(metadataLayoutVersion + 1).entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Integer, Set<LayoutFeature>> f = iterator.next();
        for (LayoutFeature lf : f.getValue()) {
          Optional<? extends LayoutFeature.UpgradeAction> upgradeAction =
              lf.onFinalizeAction();
          upgradeAction.ifPresent(action -> action.executeAction(param));
          // ToDo : Handle shutdown while iterating case (resume from last
          //  feature).
        }
        metadataLayoutVersion = f.getKey();
      }
      // ToDo : Persist new MLV.
    }
  }

  protected static synchronized void reset() {
    metadataLayoutVersion = 0;
    softwareLayoutVersion = 0;
    featureMap.clear();
    features.clear();
    isInitialized = false;
  }
}
