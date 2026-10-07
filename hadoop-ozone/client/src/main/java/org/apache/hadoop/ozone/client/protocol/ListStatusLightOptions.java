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

package org.apache.hadoop.ozone.client.protocol;

import java.util.Objects;

/**
 * Options for {@link ClientProtocol#listStatusLight(ListStatusLightOptions)}.
 * Encapsulates all parameters to allow future extensibility without breaking
 * the method signature.
 */
public final class ListStatusLightOptions {

  private final String volumeName;
  private final String bucketName;
  private final String keyName;
  private final boolean recursive;
  private final String startKey;
  private final long numEntries;
  private final boolean allowPartialPrefixes;
  // When keyName is empty (root listing), this is the original S3/list
  // prefix for STS auth. Enables LIST check on this prefix instead of "*".
  private final String listPrefix;

  private ListStatusLightOptions(Builder b) {
    this.volumeName = b.volumeName;
    this.bucketName = b.bucketName;
    this.keyName = b.keyName;
    this.recursive = b.recursive;
    this.startKey = b.startKey;
    this.numEntries = b.numEntries;
    this.allowPartialPrefixes = b.allowPartialPrefixes;
    this.listPrefix = b.listPrefix;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getKeyName() {
    return keyName;
  }

  public boolean isRecursive() {
    return recursive;
  }

  public String getStartKey() {
    return startKey;
  }

  public long getNumEntries() {
    return numEntries;
  }

  public boolean isAllowPartialPrefixes() {
    return allowPartialPrefixes;
  }

  public String getListPrefix() {
    return listPrefix;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Convenience factory for the common case (no listPrefix).
   */
  public static ListStatusLightOptions of(String volumeName, String bucketName,
      String keyName, boolean recursive, String startKey, long numEntries,
      boolean allowPartialPrefixes) {
    return builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setRecursive(recursive)
        .setStartKey(startKey)
        .setNumEntries(numEntries)
        .setAllowPartialPrefixes(allowPartialPrefixes)
        .build();
  }

  /**
   * Builder for ListStatusLightOptions.
   */
  public static final class Builder {
    private String volumeName;
    private String bucketName;
    private String keyName;
    private boolean recursive;
    private String startKey;
    private long numEntries;
    private boolean allowPartialPrefixes;
    private String listPrefix;

    public Builder setVolumeName(String volumeName) {
      this.volumeName = volumeName;
      return this;
    }

    public Builder setBucketName(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }

    public Builder setKeyName(String keyName) {
      this.keyName = keyName;
      return this;
    }

    public Builder setRecursive(boolean recursive) {
      this.recursive = recursive;
      return this;
    }

    public Builder setStartKey(String startKey) {
      this.startKey = startKey;
      return this;
    }

    public Builder setNumEntries(long numEntries) {
      this.numEntries = numEntries;
      return this;
    }

    public Builder setAllowPartialPrefixes(boolean allowPartialPrefixes) {
      this.allowPartialPrefixes = allowPartialPrefixes;
      return this;
    }

    public Builder setListPrefix(String listPrefix) {
      this.listPrefix = listPrefix;
      return this;
    }

    public ListStatusLightOptions build() {
      return new ListStatusLightOptions(this);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ListStatusLightOptions that = (ListStatusLightOptions) o;
    return recursive == that.recursive && numEntries == that.numEntries &&
        allowPartialPrefixes == that.allowPartialPrefixes && Objects.equals(volumeName, that.volumeName) &&
        Objects.equals(bucketName, that.bucketName) && Objects.equals(keyName, that.keyName) &&
        Objects.equals(startKey, that.startKey) && Objects.equals(listPrefix, that.listPrefix);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        volumeName, bucketName, keyName, recursive, startKey, numEntries, allowPartialPrefixes, listPrefix);
  }
}
