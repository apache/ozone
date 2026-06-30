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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * This class contains the necessary information to abort MPU keys.
 */
public final class OmMultipartAbortInfo {

  private final String multipartKey;
  private final String multipartOpenKey;
  private final OmMultipartKeyInfo omMultipartKeyInfo;
  private final BucketLayout bucketLayout;
  private final List<OmKeyInfo> partsKeyInfoToDelete;
  private final List<OmMultipartPartKey> partsTableKeysToDelete;

  private OmMultipartAbortInfo(String multipartKey, String multipartOpenKey,
      OmMultipartKeyInfo omMultipartKeyInfo, BucketLayout bucketLayout,
      List<OmKeyInfo> partsKeyInfoToDelete,
      List<OmMultipartPartKey> partsTableKeysToDelete) {
    this.multipartKey = multipartKey;
    this.multipartOpenKey = multipartOpenKey;
    this.omMultipartKeyInfo = omMultipartKeyInfo;
    this.bucketLayout = bucketLayout;
    this.partsKeyInfoToDelete = partsKeyInfoToDelete == null ?
        Collections.emptyList() : partsKeyInfoToDelete;
    this.partsTableKeysToDelete = partsTableKeysToDelete == null ?
        Collections.emptyList() : partsTableKeysToDelete;
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

  public List<OmKeyInfo> getPartsKeyInfoToDelete() {
    return partsKeyInfoToDelete;
  }

  public List<OmMultipartPartKey> getPartsTableKeysToDelete() {
    return partsTableKeysToDelete;
  }

  /**
   * Builder of OmMultipartAbortInfo.
   */
  public static class Builder {
    private String multipartKey;
    private String multipartOpenKey;
    private OmMultipartKeyInfo omMultipartKeyInfo;
    private BucketLayout bucketLayout;
    private List<OmKeyInfo> partsKeyInfoToDelete;
    private List<OmMultipartPartKey> partsTableKeysToDelete;

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

    public Builder setPartsKeyInfoToDelete(List<OmKeyInfo> keyInfos) {
      this.partsKeyInfoToDelete = keyInfos;
      return this;
    }

    public Builder setPartsTableKeysToDelete(
        List<OmMultipartPartKey> partKeys) {
      this.partsTableKeysToDelete = partKeys;
      return this;
    }

    public OmMultipartAbortInfo build() {
      return new OmMultipartAbortInfo(multipartKey,
          multipartOpenKey, omMultipartKeyInfo, bucketLayout,
          partsKeyInfoToDelete, partsTableKeysToDelete);
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
        this.omMultipartKeyInfo.equals(that.omMultipartKeyInfo) &&
        this.partsKeyInfoToDelete.equals(that.partsKeyInfoToDelete) &&
        this.partsTableKeysToDelete.equals(that.partsTableKeysToDelete);
  }

  @Override
  public int hashCode() {
    return Objects.hash(multipartKey, multipartOpenKey,
        bucketLayout, omMultipartKeyInfo, partsKeyInfoToDelete,
        partsTableKeysToDelete);
  }

}
