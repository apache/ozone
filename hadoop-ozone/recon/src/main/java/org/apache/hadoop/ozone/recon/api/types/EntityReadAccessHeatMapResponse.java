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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * HTTP Response wrapped for a read access based heatmap request.
 * This response is a special response format specifically for
 * heatmap representation over UI using treemap format.
 * Technically at low level this class is used to hold an
 * entity's heatmap and access metadata info for its children.
 * Sample JSON format per Entity:
 *     {
 *       "label": "hivevol1675429570",
 *       "children": [
 *         {
 *           "label": "hivebuck1675429570",
 *           "children": [
   *             {
   *               "label": "reg_path/hive_tpcds/store_sales/store_sales.dat",
   *               "size": 256,
 *                 "path": "/hivevol1675429570/hivebuck1675429570/reg_path/
 *                 hive_tpcds/store_sales/store_sales.dat",
   *               "accessCount": 129977,
   *               "color": 1
   *             }
 *            ],
 *           "size": 3072,
 *           "path": "/hivevol1675429570/hivebuck1675429570",
 *           "minAccessCount": 3195,
 *           "maxAccessCount": 129977
 *         }
 *       ],
 *       "size": 6144,
 *       "path": "/hivevol1675429570"
 *     }
 */
public class EntityReadAccessHeatMapResponse {

  /** Name of volume or bucket or full key name. */
  @JsonProperty("label")
  private String label;

  /** Path at each node level. */
  @JsonProperty("path")
  private String path;

  /**
   * This property is a list of each Entity's
   * heatmap and access metadata info.
   */
  @JsonProperty("children")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private List<EntityReadAccessHeatMapResponse> children;

  @JsonProperty("size")
  private long size;

  @JsonProperty("accessCount")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long accessCount;

  @JsonProperty("minAccessCount")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long minAccessCount;

  @JsonProperty("maxAccessCount")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long maxAccessCount;

  @JsonProperty("color")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private double color;

  public EntityReadAccessHeatMapResponse() {
    this.children = new ArrayList<>();
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public long getAccessCount() {
    return accessCount;
  }

  public void setAccessCount(long accessCount) {
    this.accessCount = accessCount;
  }

  public List<EntityReadAccessHeatMapResponse> getChildren() {
    return children;
  }

  public void setChildren(List<EntityReadAccessHeatMapResponse> children) {
    this.children = children;
  }

  public long getMinAccessCount() {
    return minAccessCount;
  }

  public void setMinAccessCount(long minAccessCount) {
    this.minAccessCount = minAccessCount;
  }

  public long getMaxAccessCount() {
    return maxAccessCount;
  }

  public void setMaxAccessCount(long maxAccessCount) {
    this.maxAccessCount = maxAccessCount;
  }

  public double getColor() {
    return color;
  }

  public void setColor(double color) {
    this.color = color;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EntityReadAccessHeatMapResponse that = (EntityReadAccessHeatMapResponse) o;
    return Objects.equals(label, that.label);
  }

  @Override
  public int hashCode() {
    return Objects.hash(label);
  }
}
