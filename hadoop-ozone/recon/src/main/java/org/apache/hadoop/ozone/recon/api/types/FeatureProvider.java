/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.api.types;

import com.google.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HEATMAP_PROVIDER_KEY;

/**
 * This class is responsible for maintaining Recon features metadata.
 */
@Singleton
public final class FeatureProvider {
  private static EnumMap<Feature, Boolean> featureSupportMap =
      new EnumMap<>(Feature.class);

  private FeatureProvider() {
  }

  /**
   * The Feature list of Recon. This list may grow in the future.
   * If this list grows bigger, then this may be loaded from Sql DB.
   */
  public enum Feature {

    HEATMAP("HeatMap");
    private String featureName;

    public String getFeatureName() {
      return featureName;
    }

    Feature(String featureName) {
      this.featureName = featureName;
    }

    public static Feature of(String featureName) {
      Feature featureEnum = Arrays.stream(Feature.values())
          .filter(feature -> feature.getFeatureName().equals(featureName))
          .findFirst().get();
      if (null == featureEnum) {
        throw new IllegalArgumentException("Unrecognized value for " +
            "Features enum: " + featureName);
      }
      return featureEnum;
    }
  }

  public static EnumMap<Feature, Boolean> getFeatureSupportMap() {
    return featureSupportMap;
  }

  public static List<Feature> getAllDisabledFeatures() {
    return getFeatureSupportMap().keySet().stream().filter(feature ->
        Boolean.TRUE.equals(getFeatureSupportMap().get(feature))).collect(
        Collectors.toList());

  }

  public static void initFeatureSupport(
      OzoneConfiguration ozoneConfiguration) {
    resetInitOfFeatureSupport();
    String heatMapProviderCls = ozoneConfiguration.get(
        OZONE_RECON_HEATMAP_PROVIDER_KEY);
    if (StringUtils.isEmpty(heatMapProviderCls)) {
      getFeatureSupportMap().put(Feature.HEATMAP, true);
    }
  }

  private static void resetInitOfFeatureSupport() {
    getFeatureSupportMap().keySet()
        .forEach(feature -> getFeatureSupportMap().put(feature, false));
  }
}
