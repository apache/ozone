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

import static org.apache.hadoop.ozone.om.helpers.OmLifecycleUtils.validateAndNormalizePrefix;
import static org.apache.hadoop.ozone.om.helpers.OmLifecycleUtils.validatePrefixLength;
import static org.apache.hadoop.ozone.om.helpers.OmLifecycleUtils.validateTagUniqAndLength;
import static org.apache.hadoop.ozone.om.helpers.OmLifecycleUtils.validateTrashPrefix;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.ozone.OzoneConsts;
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
  private final boolean directoryStylePrefix;

  private OmLifecycleRuleAndOperator() {
    throw new UnsupportedOperationException("Default constructor is not supported. Use Builder.");
  }

  private OmLifecycleRuleAndOperator(Builder builder) {
    this.tags = Collections.unmodifiableMap(new HashMap<>(builder.tags));
    this.prefix = builder.prefix;
    if (this.prefix != null) {
      this.directoryStylePrefix = this.prefix.contains(OzoneConsts.OM_KEY_PREFIX);
    } else {
      this.directoryStylePrefix = false;
    }
  }

  @Nonnull
  public Map<String, String> getTags() {
    return tags;
  }

  @Nullable
  public String getPrefix() {
    return prefix;
  }

  public boolean isDirectoryStylePrefix() {
    return directoryStylePrefix;
  }

  /**
   * Validates the OmLifecycleRuleAndOperator.
   * Ensures the following:
   *
   * @throws OMException if the validation fails.
   */
  public void valid(BucketLayout layout) throws OMException {
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

    if (hasTags) {
      validateTagUniqAndLength(tags);
    }

    if (hasPrefix) {
      validatePrefixLength(prefix);
      validateTrashPrefix(prefix);
    }

    if (hasPrefix && layout == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      validateAndNormalizePrefix(prefix);
    }
  }

  public boolean match(OmKeyInfo omKeyInfo, String keyPath) {
    if (prefix != null && !keyPath.startsWith(prefix)) {
      return false;
    }

    Map<String, String> keyTagList = omKeyInfo.getTags();
    for (Map.Entry<String, String> tag: tags.entrySet()) {
      String value = keyTagList.get(tag.getKey());
      if (value == null || !value.equals(tag.getValue())) {
        return false;
      }
    }

    return true;
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

    public OmLifecycleRuleAndOperator build() {
      return new OmLifecycleRuleAndOperator(this);
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

  public static OmLifecycleRuleAndOperator getFromProtobuf(LifecycleRuleAndOperator andOperator,
      BucketLayout layout) {
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
