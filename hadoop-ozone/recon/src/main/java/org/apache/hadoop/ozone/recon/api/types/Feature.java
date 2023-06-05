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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The Feature list of Recon. This list may grow in the future.
 * If this list grows bigger, then this may be loaded from Sql DB.
 */
public enum Feature {
  HEATMAP("HeatMap", false) {
    private boolean disabled;
    @Override
    public boolean isDisabled() {
      return this.disabled;
    }
    @Override
    public void setDisabled(boolean disabled) {
      this.disabled = disabled;
    }
  };
  private String featureName;
  private boolean disabled;

  public boolean isDisabled() {
    return disabled;
  }

  public void setDisabled(boolean disabled) {
    this.disabled = disabled;
  }

  public String getFeatureName() {
    return featureName;
  }

  Feature(String featureName, boolean disabled) {
    this.featureName = featureName;
    this.disabled = disabled;
  }

  public static Feature of(String featureName) {
    if (featureName.equals("HeatMap")) {
      return HEATMAP;
    }
    throw new IllegalArgumentException("Unrecognized value for " +
        "Features enum: " + featureName);
  }

  public String toString() {
    switch (this) {
    case HEATMAP:
      return "HeatMap";
    default:
      return "";
    }
  }

  public static List<Feature> getAllDisabledFeatures() {
    return Arrays.stream(Feature.values())
        .filter(feature -> feature.isDisabled())
        .collect(Collectors.toList());
  }
}
