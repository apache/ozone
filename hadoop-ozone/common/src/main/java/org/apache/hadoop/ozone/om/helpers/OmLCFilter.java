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

import jakarta.annotation.Nullable;
import net.jcip.annotations.Immutable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.exceptions.OMException;

/**
 * A class that encapsulates lifecycle rule filter.
 * At the moment only prefix is supported in filter.
 */
@Immutable
public final class OmLCFilter {

  private final String prefix;
  private final String tagKey;
  private final String tagValue;
  private final OmLifecycleRuleAndOperator andOperator;

  private OmLCFilter() {
    throw new UnsupportedOperationException("Default constructor is not supported. Use Builder.");
  }

  private OmLCFilter(Builder builder) {
    this.prefix = builder.prefix;
    this.andOperator = builder.andOperator;
    this.tagKey = builder.tagKey;
    this.tagValue = builder.tagValue;
  }

  /**
   * Validates the OmLCFilter.
   * Ensures that only one of prefix, tag, or andOperator is set.
   * You can specify an empty filter, in which case the rule applies to all objects in the bucket.
   * Ref: <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-filters.html#filter-examples">...</a>
   * If the validation fails, an OMException is thrown.
   *
   * @throws OMException if the filter is invalid.
   */
  public void valid() throws OMException {
    boolean hasPrefix = prefix != null;
    boolean hasTag = tagKey != null && tagValue != null;
    boolean hasAndOperator = andOperator != null;

    if ((hasPrefix && (hasTag || hasAndOperator)) || (hasTag && hasAndOperator)) {
      throw new OMException("Invalid lifecycle filter configuration: Only one of 'Prefix'," +
          " 'Tag', or 'AndOperator' should be specified.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (andOperator != null) {
      andOperator.valid();
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
    return Pair.of(tagKey, tagValue);
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

    public OmLCFilter build() throws OMException {
      OmLCFilter omLCFilter = new OmLCFilter(this);
      omLCFilter.valid();
      return omLCFilter;
    }
  }

}
