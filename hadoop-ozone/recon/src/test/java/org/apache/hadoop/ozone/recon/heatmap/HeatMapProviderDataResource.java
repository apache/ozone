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

package org.apache.hadoop.ozone.recon.heatmap;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.ozone.recon.api.types.EntityMetaData;

import java.util.Arrays;

/**
 * This class is used to encapsulate entity's access metadata objects.
 */
public class HeatMapProviderDataResource {
  @JsonProperty("buckets")
  private EntityMetaData[] metaDataList;

  public EntityMetaData[] getMetaDataList() {
    if (null != metaDataList) {
      return Arrays.copyOfRange(metaDataList, 0, metaDataList.length);
    }
    return null;
  }

  public void setMetaDataList(EntityMetaData[] metaDataList) {
    this.metaDataList = Arrays.copyOfRange(metaDataList, 0,
        metaDataList.length);
  }
}
