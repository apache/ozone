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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.exceptions.OMException;

/**
 * A class that encapsulates lifecycle rule filter.
 * At the moment only prefix is supported in filter.
 */
public final class OmLCFilter {

  private final String prefix;
  private final Pair<String, String> tag;
  private final OmLifecycleRuleAndOperator andOperator;

  public OmLifecycleRuleAndOperator getAndOperator() {
    return andOperator;
  }

  private OmLCFilter(String prefix, Pair<String, String> tag,
      OmLifecycleRuleAndOperator andOperator) {
    this.prefix = prefix;
    this.andOperator = andOperator;
    this.tag = tag;
  }

  /**
   * Validates the OmLCFilter.
   * Ensures that only one of prefix, tag, or andOperator is set.
   * If the validation fails, an OMException is thrown.
   *
   * @throws OMException if the filter is invalid.
   */
  public void valid() throws OMException {
    boolean hasPrefix = prefix != null;
    boolean hasTag = tag != null;
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

  @Nullable
  public String getPrefix() {
    return prefix;
  }

  @Nullable
  public Pair<String, String> getTag() {
    return tag;
  }

  @Override
  public String toString() {
    return "OmLCFilter{" +
        "prefix='" + prefix + '\'' +
        ", tag=" + tag +
        ", andOperator=" + andOperator +
        '}';
  }

  /**
   * Builder of OmLCFilter.
   */
  public static class Builder {
    private String prefix = null;
    private Pair<String, String> tag = null;
    private OmLifecycleRuleAndOperator andOperator = null;

    public Builder setAndOperator(OmLifecycleRuleAndOperator andOp) {
      this.andOperator = andOp;
      return this;
    }

    public Builder setPrefix(String lcPrefix) {
      this.prefix = lcPrefix;
      return this;
    }

    public Builder setTag(String key, String value) {
      this.tag = Pair.of(key, value);
      return this;
    }

    public OmLCFilter build() {
      return new OmLCFilter(prefix, tag, andOperator);
    }
  }

}
