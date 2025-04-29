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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleFilterTag;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleRuleAndOperator;

/**
 * A class that encapsulates lifecycleRule andOperator.
 */
@Immutable
public final class OmLifecycleRuleAndOperator {

  private final Map<String, String> tags;
  private final String prefix;

  private OmLifecycleRuleAndOperator() {
    throw new UnsupportedOperationException("Default constructor is not supported. Use Builder.");
  }

  private OmLifecycleRuleAndOperator(Builder builder) {
    this.tags = Collections.unmodifiableMap(new HashMap<>(builder.tags));
    this.prefix = builder.prefix;
  }

  @Nonnull
  public Map<String, String> getTags() {
    return tags;
  }

  @Nullable
  public String getPrefix() {
    return prefix;
  }

  /**
   * Validates the OmLifecycleRuleAndOperator.
   * Ensures the following:
   * - Either tags or prefix must be specified.
   * - If there are tags and no prefix, the tags should be more than one.
   * - Prefix can be "".
   * - Prefix alone is not allowed.
   *
   * @throws OMException if the validation fails.
   */
  public void valid() throws OMException {
    boolean hasTags = tags != null && !tags.isEmpty();
    boolean hasPrefix = prefix != null;

    if (!hasTags && !hasPrefix) {
      throw new OMException("Invalid lifecycle rule andOperator configuration: " +
          "Either 'Tags' or 'Prefix' must be specified.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (hasTags && !hasPrefix && tags.size() == 1) {
      throw new OMException("Invalid lifecycle rule andOperator configuration: " +
          "If 'Tags' are specified without 'Prefix', there should be more than one tag.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (hasPrefix && !hasTags) {
      throw new OMException("Invalid lifecycle rule andOperator configuration: " +
          "'Prefix' alone is not allowed.",
          OMException.ResultCodes.INVALID_REQUEST);
    }
  }


  /**
   * The builder for the OmLifecycleRuleAndOperator class.
   */
  public static class Builder {
    private Map<String, String> tags = new HashMap<>();
    private String prefix;

    public Builder setPrefix(String lcPrefix) {
      this.prefix = lcPrefix;
      return this;
    }

    public Builder addTag(String key, String value) {
      this.tags.put(key, value);
      return this;
    }

    public Builder setTags(Map<String, String> lcTags) {
      if (lcTags != null) {
        this.tags = new HashMap<>(lcTags);
      }
      return this;
    }

    public OmLifecycleRuleAndOperator build() throws OMException {
      OmLifecycleRuleAndOperator omLifecycleRuleAndOperator = new OmLifecycleRuleAndOperator(this);
      omLifecycleRuleAndOperator.valid();
      return omLifecycleRuleAndOperator;
    }
  }

  public LifecycleRuleAndOperator getProtobuf() {
    LifecycleRuleAndOperator.Builder andOpBuilder = LifecycleRuleAndOperator.newBuilder();

    if (tags != null) {
      andOpBuilder.addAllTags(
          tags.entrySet().stream()
              .map(lcTag ->
                  LifecycleFilterTag.newBuilder()
                      .setKey(lcTag.getKey())
                      .setValue(lcTag.getValue())
                      .build())
              .collect(Collectors.toList()));
    }
    if (prefix != null) {
      andOpBuilder.setPrefix(prefix);
    }

    return andOpBuilder.build();
  }

  public static OmLifecycleRuleAndOperator getFromProtobuf(LifecycleRuleAndOperator andOperator) throws OMException {
    OmLifecycleRuleAndOperator.Builder builder = new OmLifecycleRuleAndOperator.Builder();

    if (andOperator.hasPrefix()) {
      builder.setPrefix(andOperator.getPrefix());
    }
    andOperator.getTagsList().forEach(tag -> {
      builder.addTag(tag.getKey(), tag.getValue());
    });

    return builder.build();
  }

  @Override
  public String toString() {
    return "OmLifecycleRuleAndOperator{" +
        "prefix='" + prefix + '\'' +
        ", tags=" + tags +
        '}';
  }
}
