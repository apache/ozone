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

package org.apache.hadoop.ozone.om.helpers;

import java.util.Set;
 
/**
 * This class is used to store the result of OmMetadataManager#getMultipartUploadKeys.
 */
public class MultipartUploadKeys {
  private final Set<String> keys;
  private final String nextKeyMarker;
  private final String nextUploadIdMarker;
  private final boolean isTruncated;

  public MultipartUploadKeys(Set<String> keys, String nextKeyMarker, String nextUploadIdMarker, boolean isTruncated) {
    this.keys = keys;
    this.nextKeyMarker = nextKeyMarker;
    this.nextUploadIdMarker = nextUploadIdMarker;
    this.isTruncated = isTruncated;
  }

  public Set<String> getKeys() {
    return keys;
  }

  public String getNextKeyMarker() {
    return nextKeyMarker;
  }

  public String getNextUploadIdMarker() {
    return nextUploadIdMarker;
  }

  public boolean isTruncated() {
    return isTruncated;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for MultipartUploadKeys.
   */
  public static class Builder {
    private Set<String> keys;
    private String nextKeyMarker = "";
    private String nextUploadIdMarker = "";
    private boolean isTruncated;

    public Builder setKeys(Set<String> keys) {
      this.keys = keys;
      return this;
    }

    public Builder setNextKeyMarker(String nextKeyMarker) {
      this.nextKeyMarker = nextKeyMarker;
      return this;
    }

    public Builder setNextUploadIdMarker(String nextUploadIdMarker) {
      this.nextUploadIdMarker = nextUploadIdMarker;
      return this;
    }

    public Builder setIsTruncated(boolean isTruncated) {
      this.isTruncated = isTruncated;
      return this;
    }

    public MultipartUploadKeys build() {
      return new MultipartUploadKeys(keys, nextKeyMarker, nextUploadIdMarker, isTruncated);
    }

  }
}
