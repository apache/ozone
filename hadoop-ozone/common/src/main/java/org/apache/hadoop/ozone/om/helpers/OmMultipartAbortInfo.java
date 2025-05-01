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

import java.util.Objects;

/**
 * This class contains the necessary information to abort MPU keys.
 */
public final class OmMultipartAbortInfo {

  private final String multipartKey;
  private final String multipartOpenKey;
  private final OmMultipartKeyInfo omMultipartKeyInfo;
  private final BucketLayout bucketLayout;

  private OmMultipartAbortInfo(String multipartKey, String multipartOpenKey,
      OmMultipartKeyInfo omMultipartKeyInfo, BucketLayout bucketLayout) {
    this.multipartKey = multipartKey;
    this.multipartOpenKey = multipartOpenKey;
    this.omMultipartKeyInfo = omMultipartKeyInfo;
    this.bucketLayout = bucketLayout;
  }

  public String getMultipartKey() {
    return multipartKey;
  }

  public String getMultipartOpenKey() {
    return multipartOpenKey;
  }

  public OmMultipartKeyInfo getOmMultipartKeyInfo() {
    return omMultipartKeyInfo;
  }

  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }

  /**
   * Builder of OmMultipartAbortInfo.
   */
  public static class Builder {
    private String multipartKey;
    private String multipartOpenKey;
    private OmMultipartKeyInfo omMultipartKeyInfo;
    private BucketLayout bucketLayout;

    public Builder setMultipartKey(String mpuKey) {
      this.multipartKey = mpuKey;
      return this;
    }

    public Builder setMultipartOpenKey(String mpuOpenKey) {
      this.multipartOpenKey = mpuOpenKey;
      return this;
    }

    public Builder setMultipartKeyInfo(OmMultipartKeyInfo multipartKeyInfo) {
      this.omMultipartKeyInfo = multipartKeyInfo;
      return this;
    }

    public Builder setBucketLayout(BucketLayout layout) {
      this.bucketLayout = layout;
      return this;
    }

    public OmMultipartAbortInfo build() {
      return new OmMultipartAbortInfo(multipartKey,
          multipartOpenKey, omMultipartKeyInfo, bucketLayout);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    final OmMultipartAbortInfo that = (OmMultipartAbortInfo) other;

    return this.multipartKey.equals(that.multipartKey) &&
        this.multipartOpenKey.equals(that.multipartOpenKey) &&
        this.bucketLayout.equals(that.bucketLayout) &&
        this.omMultipartKeyInfo.equals(that.omMultipartKeyInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(multipartKey, multipartOpenKey,
        bucketLayout, omMultipartKeyInfo);
  }

}
