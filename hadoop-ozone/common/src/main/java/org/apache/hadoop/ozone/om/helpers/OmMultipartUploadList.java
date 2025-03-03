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

import java.util.List;

/**
 * List of in-flight MPU uploads.
 */
public final class OmMultipartUploadList {

  private List<OmMultipartUpload> uploads;
  private String nextKeyMarker;
  private String nextUploadIdMarker;
  private boolean isTruncated;

  private OmMultipartUploadList(Builder builder) {
    this.uploads = builder.uploads;
    this.nextKeyMarker = builder.nextKeyMarker;
    this.nextUploadIdMarker = builder.nextUploadIdMarker;
    this.isTruncated = builder.isTruncated;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public List<OmMultipartUpload> getUploads() {
    return uploads;
  }

  public void setUploads(
      List<OmMultipartUpload> uploads) {
    this.uploads = uploads;
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

  /**
   * Builder class for OmMultipartUploadList.
   */
  public static class Builder {
    private List<OmMultipartUpload> uploads;
    private String nextKeyMarker = "";
    private String nextUploadIdMarker = "";
    private boolean isTruncated;

    public Builder() {
    }

    public Builder setUploads(List<OmMultipartUpload> uploads) {
      this.uploads = uploads;
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

    public OmMultipartUploadList build() {
      return new OmMultipartUploadList(this);
    }
  }
}
