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
 * Represents information about the open keys in a storage system.
 * This class provides details regarding different types of open key bytes
 * and calculates the total size of open key bytes.
 */
public class OpenKeyBytesInfo {

  @JsonProperty("openKeyAndFileBytes")
  private long openKeyAndFileBytes;

  @JsonProperty("multipartOpenKeyBytes")
  private long multipartOpenKeyBytes;

  @JsonProperty("totalOpenKeyBytes")
  private long totalOpenKeyBytes;

  public OpenKeyBytesInfo() { }

  public OpenKeyBytesInfo(long openKeyAndFileBytes, long multipartOpenKeyBytes) {
    this.openKeyAndFileBytes = openKeyAndFileBytes;
    this.multipartOpenKeyBytes = multipartOpenKeyBytes;
  }

  public long getTotalOpenKeyBytes() {
    return openKeyAndFileBytes + multipartOpenKeyBytes;
  }

  public long getOpenKeyAndFileBytes() {
    return openKeyAndFileBytes;
  }

  public long getMultipartOpenKeyBytes() {
    return multipartOpenKeyBytes;
  }
}
