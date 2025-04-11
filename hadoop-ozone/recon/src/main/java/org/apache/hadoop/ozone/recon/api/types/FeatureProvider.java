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

package org.apache.hadoop.ozone.recon.api.types;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HEATMAP_ENABLE_DEFAULT;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HEATMAP_ENABLE_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HEATMAP_PROVIDER_KEY;

import com.google.inject.Singleton;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

/**
 * This class is responsible for maintaining Recon features metadata.
 */
@Singleton
public final class FeatureProvider {
  private static EnumMap<Feature, Boolean> featureDisableMap =
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

  public static EnumMap<Feature, Boolean> getFeatureDisableMap() {
    return featureDisableMap;
  }

  public static List<Feature> getAllDisabledFeatures() {
    return getFeatureDisableMap().keySet().stream().filter(feature ->
        Boolean.TRUE.equals(getFeatureDisableMap().get(feature))).collect(
        Collectors.toList());

  }

  public static void initFeatureSupport(
      OzoneConfiguration ozoneConfiguration) {
    resetInitOfFeatureSupport();
    String heatMapProviderCls = ozoneConfiguration.get(
        OZONE_RECON_HEATMAP_PROVIDER_KEY);
    boolean heatMapEnabled = ozoneConfiguration.getBoolean(
        OZONE_RECON_HEATMAP_ENABLE_KEY, OZONE_RECON_HEATMAP_ENABLE_DEFAULT);
    if (!heatMapEnabled || StringUtils.isEmpty(heatMapProviderCls)) {
      getFeatureDisableMap().put(Feature.HEATMAP, true);
    }
  }

  private static void resetInitOfFeatureSupport() {
    getFeatureDisableMap().keySet()
        .forEach(feature -> getFeatureDisableMap().put(feature, false));
  }
}
