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
 * The GlobalNamespaceReport class serves as a representation of
 * the global namespace metadata summary for a storage system.
 * This class encapsulates statistical information related
 * to the state of the global namespace.
 *
 * The metadata includes:
 * - Total space utilized.
 * - Total number of keys present in the namespace.
 */
public class GlobalNamespaceReport {

  @JsonProperty("totalUsedSpace")
  private long totalUsedSpace;

  @JsonProperty("totalKeys")
  private long totalKeys;

  public GlobalNamespaceReport() {
  }

  public GlobalNamespaceReport(long totalUsedSpace, long totalKeys) {
    this.totalUsedSpace = totalUsedSpace;
    this.totalKeys = totalKeys;
  }

  public long getTotalUsedSpace() {
    return totalUsedSpace;
  }

  public long getTotalKeys() {
    return totalKeys;
  }
}
