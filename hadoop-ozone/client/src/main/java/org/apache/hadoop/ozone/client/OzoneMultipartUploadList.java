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

package org.apache.hadoop.ozone.client;

import java.util.List;
import java.util.Objects;

/**
 * List of in-flight MPU upoads.
 */
public class OzoneMultipartUploadList {

  private List<OzoneMultipartUpload> uploads;
  private String nextKeyMarker;
  private String nextUploadIdMarker;
  private boolean isTruncated;

  public OzoneMultipartUploadList(
      List<OzoneMultipartUpload> uploads,
      String nextKeyMarker,
      String nextUploadIdMarker,
      boolean isTruncated) {
    Objects.requireNonNull(uploads, "uploads == null");
    this.uploads = uploads;
    this.nextKeyMarker = nextKeyMarker;
    this.nextUploadIdMarker = nextUploadIdMarker;
    this.isTruncated = isTruncated;
  }

  public List<OzoneMultipartUpload> getUploads() {
    return uploads;
  }

  public void setUploads(
      List<OzoneMultipartUpload> uploads) {
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
}
