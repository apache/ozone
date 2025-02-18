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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class is used as a placeholder for entity's audit log related metadata.
 */
public class EntityMetaData {
  private String val;
  private int count;
  @JsonProperty("read_access_count")
  private int readAccessCount;

  public String getVal() {
    return val;
  }

  public void setVal(String val) {
    this.val = val;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public int getReadAccessCount() {
    return readAccessCount;
  }

  public void setReadAccessCount(int readAccessCount) {
    this.readAccessCount = readAccessCount;
  }
}
