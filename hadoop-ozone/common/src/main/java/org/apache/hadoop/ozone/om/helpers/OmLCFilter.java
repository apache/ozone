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

import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.isValidKeyPath;
import static org.apache.hadoop.ozone.om.helpers.OzoneFSUtils.normalizePrefix;

import jakarta.annotation.Nullable;
import net.jcip.annotations.Immutable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleFilter;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleFilterTag;

/**
 * A class that encapsulates lifecycle rule filter.
 * At the moment only prefix is supported in filter.
 */
@Immutable
public final class OmLCFilter {

  private final String prefix;
  private final boolean directoryStylePrefix;
  private final String tagKey;
  private final String tagValue;
  private final OmLifecycleRuleAndOperator andOperator;

  private OmLCFilter() {
    throw new UnsupportedOperationException("Default constructor is not supported. Use Builder.");
  }

  private OmLCFilter(Builder builder) {
    this.prefix = builder.prefix;
    if (this.prefix != null) {
      this.directoryStylePrefix = this.prefix.contains(OzoneConsts.OM_KEY_PREFIX);
    } else {
      this.directoryStylePrefix = false;
    }
    this.andOperator = builder.andOperator;
    this.tagKey = builder.tagKey;
    this.tagValue = builder.tagValue;
  }

  /**
   * Validates the OmLCFilter.
   * Ensures that only one of prefix, tag, or andOperator is set.
   * You can specify an empty filter, in which case the rule applies to all objects in the bucket.
   * Prefix can be "", in which case the rule applies to all objects in the bucket.
   * Ref: <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-filters.html#filter-examples">...</a>
   * If the validation fails, an OMException is thrown.
   *
   * @throws OMException if the filter is invalid.
   */
  public void valid(BucketLayout layout) throws OMException {
    boolean hasPrefix = prefix != null;
    boolean hasTag = hasTag();
    boolean hasAndOperator = andOperator != null;

    if ((hasPrefix && (hasTag || hasAndOperator)) || (hasTag && hasAndOperator)) {
      throw new OMException("Invalid lifecycle filter configuration: Only one of 'Prefix'," +
          " 'Tag', or 'AndOperator' should be specified.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (hasPrefix && layout == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      String normalizedPrefix = normalizePrefix(prefix);
      if (!normalizedPrefix.equals(prefix)) {
        throw new OMException("Prefix format is not supported. Please use " + normalizedPrefix +
            " instead of " + prefix + ".", OMException.ResultCodes.INVALID_REQUEST);
      }
      try {
        isValidKeyPath(normalizedPrefix);
      } catch (OMException e) {
        throw new OMException("Prefix is not a valid key path: " + prefix, OMException.ResultCodes.INVALID_REQUEST);
      }
    }

    if (andOperator != null) {
      andOperator.valid(layout);
    }
  }

  public OmLifecycleRuleAndOperator getAndOperator() {
    return andOperator;
  }

  @Nullable
  public String getPrefix() {
    return prefix;
  }

  @Nullable
  public Pair<String, String> getTag() {
    if (hasTag()) {
      return Pair.of(tagKey, tagValue);
    }
    return null;
  }

  public boolean match(OmKeyInfo omKeyInfo) {
    return match(omKeyInfo, omKeyInfo.getKeyName());
  }

  public boolean match(OmKeyInfo omKeyInfo, String keyPath) {
    if (prefix != null) {
      return keyPath.startsWith(prefix);
    } else if (hasTag()) {
      String value = omKeyInfo.getTags().get(tagKey);
      return (value != null && value.equals(tagValue));
    } else if (andOperator != null) {
      return andOperator.match(omKeyInfo, keyPath);
    } else {
      // both prefix, tag, and andOperator are null
      return true;
    }
  }

  public boolean match(OmDirectoryInfo dirInfo, String keyPath) {
    if (prefix != null) {
      return keyPath.startsWith(prefix);
    } else {
      // directory doesn't support tag
      // if prefix, tag, and andOperator are all null, means empty filter which covers all keys/directory under bucket
      return !(hasTag() || andOperator != null);
    }
  }

  public boolean isDirectoryStylePrefix() {
    return directoryStylePrefix || (andOperator != null ? andOperator.isDirectoryStylePrefix() : false);
  }

  public LifecycleFilter getProtobuf() {
    LifecycleFilter.Builder filterBuilder = LifecycleFilter.newBuilder();

    if (prefix != null) {
      filterBuilder.setPrefix(prefix);
    }
    if (hasTag()) {
      filterBuilder.setTag(LifecycleFilterTag.newBuilder()
          .setKey(tagKey)
          .setValue(tagValue)
          .build());
    }
    if (andOperator != null) {
      filterBuilder.setAndOperator(andOperator.getProtobuf());
    }

    return filterBuilder.build();
  }

  public static OmLCFilter getFromProtobuf(LifecycleFilter lifecycleFilter, BucketLayout layout) {
    OmLCFilter.Builder builder = new Builder();

    if (lifecycleFilter.hasPrefix()) {
      builder.setPrefix(lifecycleFilter.getPrefix());
    }
    if (lifecycleFilter.hasTag()) {
      builder.setTag(lifecycleFilter.getTag().getKey(), lifecycleFilter.getTag().getValue());
    }
    if (lifecycleFilter.hasAndOperator()) {
      builder.setAndOperator(
          OmLifecycleRuleAndOperator.getFromProtobuf(lifecycleFilter.getAndOperator(), layout));
    }

    return builder.build();
  }

  @Override
  public String toString() {
    return "OmLCFilter{" +
        "prefix='" + prefix + '\'' +
        ", tagKey='" + tagKey + '\'' +
        ", tagValue='" + tagValue + '\'' +
        ", andOperator=" + andOperator +
        '}';
  }

  private boolean hasTag() {
    return tagKey != null && tagValue != null;
  }

  /**
   * Builder of OmLCFilter.
   */
  public static class Builder {
    private String prefix = null;
    private String tagKey = null;
    private String tagValue = null;
    private OmLifecycleRuleAndOperator andOperator = null;

    public Builder setPrefix(String lcPrefix) {
      this.prefix = lcPrefix;
      return this;
    }

    public Builder setTag(String key, String value) {
      this.tagKey = key;
      this.tagValue = value;
      return this;
    }

    public Builder setAndOperator(OmLifecycleRuleAndOperator andOp) {
      this.andOperator = andOp;
      return this;
    }

    public OmLCFilter build() {
      return new OmLCFilter(this);
    }
  }
}
