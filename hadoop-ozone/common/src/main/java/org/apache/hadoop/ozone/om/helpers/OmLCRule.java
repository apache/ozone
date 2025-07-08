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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.jcip.annotations.Immutable;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleAction;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleRule;

/**
 * A class that encapsulates lifecycle rule.
 */
@Immutable
public final class OmLCRule {

  public static final int LC_ID_LENGTH = 48;
  // Ref: https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html#intro-lifecycle-rule-id
  public static final int LC_ID_MAX_LENGTH = 255;

  private final String id;
  private final String prefix;
  private final String canonicalPrefix;
  private final boolean directoryStylePrefix;
  private final boolean enabled;
  // List of actions for this rule
  private final List<OmLCAction> actions;
  private final OmLCFilter filter;

  private final boolean isPrefixEnable;
  private final boolean isTagEnable;

  private OmLCRule() {
    throw new UnsupportedOperationException("Default constructor is not supported. Use Builder.");
  }

  private OmLCRule(Builder builder) {
    this.prefix = builder.prefix;
    this.canonicalPrefix = builder.canonicalPrefix;
    if (this.canonicalPrefix != null) {
      this.directoryStylePrefix = this.canonicalPrefix.contains(OzoneConsts.OM_KEY_PREFIX);
    } else {
      this.directoryStylePrefix = false;
    }
    this.enabled = builder.enabled;
    this.actions = Collections.unmodifiableList(new ArrayList<>(builder.actions));
    this.filter = builder.filter;
    // If no ID is specified in the lifecycle configure, a random ID will be generated
    if (StringUtils.isEmpty(builder.id)) {
      this.id = RandomStringUtils.randomAlphanumeric(LC_ID_LENGTH);
    } else {
      this.id = builder.id;
    }

    OmLifecycleRuleAndOperator andOperator = filter != null ? filter.getAndOperator() : null;

    this.isPrefixEnable = prefix != null ||
        (filter != null && filter.getPrefix() != null) ||
        (andOperator != null && andOperator.getPrefix() != null);

    this.isTagEnable = (filter != null && filter.getTag() != null) ||
        (andOperator != null && !andOperator.getTags().isEmpty());
  }


  public String getId() {
    return id;
  }

  @Nullable
  public String getEffectivePrefix() {
    return prefix != null ? prefix :
        (filter != null && filter.getPrefix() != null) ? filter.getPrefix() :
        (filter != null && filter.getAndOperator() != null && filter.getAndOperator().getPrefix() != null) ?
            filter.getAndOperator().getPrefix() : null;
  }

  @Nullable
  public String getEffectiveCanonicalPrefix() {
    return canonicalPrefix != null ? canonicalPrefix :
        (filter != null && filter.getPrefix() != null) ? filter.getCanonicalPrefix() :
            (filter != null && filter.getAndOperator() != null && filter.getAndOperator().getPrefix() != null) ?
                filter.getAndOperator().getCanonicalPrefix() : null;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public List<OmLCAction> getActions() {
    return actions;
  }

  /**
   * Get the expiration action if present.
   *
   * @return the expiration action if present, null otherwise
   */
  @Nullable
  public OmLCExpiration getExpiration() {
    for (OmLCAction action : actions) {
      if (action instanceof OmLCExpiration) {
        return (OmLCExpiration) action;
      }
    }
    return null;
  }

  @Nullable
  public OmLCFilter getFilter() {
    return filter;
  }

  public boolean isPrefixEnable() {
    return isPrefixEnable;
  }

  @Nullable
  public boolean isDirectoryStylePrefix() {
    return directoryStylePrefix || (filter != null ? filter.isDirectoryStylePrefix() : false);
  }

  public boolean isTagEnable() {
    return isTagEnable;
  }

  /**
   * Validates the lifecycle rule.
   * - ID length should not exceed the allowed limit
   * - At least one action must be specified
   * - Filter and Prefix cannot be used together
   * - Filter and prefix cannot both be null
   * - Prefix can be "", in which case the rule applies to all objects in the bucket.
   * - Actions must be valid
   * - Filter must be valid
   * - There must be at most one Expiration action per rule
   *
   * @throws OMException if the validation fails
   */
  public void valid(BucketLayout bucketLayout) throws OMException {
    if (id.length() > LC_ID_MAX_LENGTH) {
      throw new OMException("ID length should not exceed allowed limit of " + LC_ID_MAX_LENGTH,
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (actions == null || actions.isEmpty()) {
      throw new OMException("At least one action needs to be specified in a rule.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    // Check that there is at most one Expiration action
    int expirationActionCount = 0;
    for (OmLCAction action : actions) {
      if (action.getActionType() == OmLCAction.ActionType.EXPIRATION) {
        expirationActionCount++;
      }
      if (expirationActionCount > 1) {
        throw new OMException("A rule can have at most one Expiration action.",
            OMException.ResultCodes.INVALID_REQUEST);
      }
      action.valid();
    }

    if (prefix != null && filter != null) {
      throw new OMException("Filter and Prefix cannot be used together.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (prefix == null && filter == null) {
      throw new OMException("Filter and Prefix cannot both be null.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (prefix != null && bucketLayout == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      isValidKeyPath(normalizePrefix(prefix));
    }

    if (filter != null) {
      filter.valid(bucketLayout);
    }
  }

  /**
   *
   * @param omKeyInfo detail Key info to evaluate against this rule
   * @return true is this key fits this rule and will trigger the action, otherwise false
   */
  public boolean match(OmKeyInfo omKeyInfo) {
    boolean matched = false;
    // verify modification time first
    if (getExpiration().isExpired(omKeyInfo.getModificationTime())) {
      // verify prefix and filter
      if (prefix != null) {
        if (omKeyInfo.getKeyName().startsWith(canonicalPrefix)) {
          matched = true;
        }
      } else {
        return filter.match(omKeyInfo);
      }
    }
    return matched;
  }

  /**
   *
   * @param omKeyInfo detail Key info to evaluate against this rule
   * @param keyPath path include key name and all its parent, except bucket and volume
   * @return true is this key fits this rule and will trigger the action, otherwise false
   */
  public boolean match(OmKeyInfo omKeyInfo, String keyPath) {
    boolean matched = false;
    // verify modification time first
    if (getExpiration().isExpired(omKeyInfo.getModificationTime())) {
      // verify prefix and filter
      if (prefix != null) {
        if (keyPath.startsWith(canonicalPrefix)) {
          matched = true;
        }
      } else {
        return filter.match(omKeyInfo, keyPath);
      }
    }
    return matched;
  }

  public boolean match(OmDirectoryInfo dirInfo, String keyPath) {
    boolean matched = false;
    // verify modification time first
    if (getExpiration().isExpired(dirInfo.getModificationTime())) {
      // verify prefix and filter
      if (prefix != null) {
        if (keyPath.startsWith(canonicalPrefix)) {
          matched = true;
        }
      } else {
        return filter.match(dirInfo, keyPath);
      }
    }
    return matched;
  }

  public LifecycleRule getProtobuf() {
    LifecycleRule.Builder builder = LifecycleRule.newBuilder()
        .setId(id)
        .setEnabled(enabled);

    if (prefix != null) {
      builder.setPrefix(prefix);
    }
    if (actions != null) {
      for (OmLCAction action : actions) {
        builder.addAction(action.getProtobuf());
      }
    }
    if (filter != null) {
      builder.setFilter(filter.getProtobuf());
    }

    return builder.build();
  }

  public static OmLCRule getFromProtobuf(LifecycleRule lifecycleRule, BucketLayout layout) throws OMException {
    Builder builder = new Builder()
        .setEnabled(lifecycleRule.getEnabled());

    if (lifecycleRule.hasId()) {
      builder.setId(lifecycleRule.getId());
    }
    if (lifecycleRule.hasPrefix()) {
      String prefix = lifecycleRule.getPrefix();
      builder.setPrefix(prefix);
      if (layout == BucketLayout.FILE_SYSTEM_OPTIMIZED && prefix.startsWith(OzoneConsts.OM_KEY_PREFIX)) {
        String normalizedKeyName = normalizePrefix(prefix);
        isValidKeyPath(normalizedKeyName);
        builder.setCanonicalPrefix(normalizedKeyName);
      } else {
        builder.setCanonicalPrefix(prefix);
      }
    }
    for (LifecycleAction lifecycleAction : lifecycleRule.getActionList()) {
      if (lifecycleAction.hasExpiration()) {
        builder.addAction(OmLCExpiration.getFromProtobuf(lifecycleAction.getExpiration()));
      }
    }
    if (lifecycleRule.hasFilter()) {
      builder.setFilter(OmLCFilter.getFromProtobuf(lifecycleRule.getFilter(), layout));
    }

    return builder.setBucketLayout(layout).build();
  }

  @Override
  public String toString() {
    return "OmLCRule{" +
        "id='" + id + '\'' +
        ", prefix='" + prefix + '\'' +
        ", canonicalPrefix='" + canonicalPrefix + '\'' +
        ", enabled=" + enabled +
        ", isPrefixEnable=" + isPrefixEnable +
        ", isTagEnable=" + isTagEnable +
        ", actions=" + actions +
        ", filter=" + filter +
        '}';
  }

  /**
   * Builder of OmLCRule.
   */
  public static class Builder {
    private String id = "";
    private String prefix;
    private String canonicalPrefix;
    private boolean enabled;
    private List<OmLCAction> actions = new ArrayList<>();
    private OmLCFilter filter;
    private BucketLayout bucketLayout;

    public Builder setId(String lcId) {
      this.id = lcId;
      return this;
    }

    public Builder setPrefix(String lcPrefix) {
      this.prefix = lcPrefix;
      return this;
    }

    public Builder setCanonicalPrefix(String canonicalPrefix) {
      this.canonicalPrefix = canonicalPrefix;
      return this;
    }

    public Builder setEnabled(boolean lcEnabled) {
      this.enabled = lcEnabled;
      return this;
    }

    public Builder setAction(OmLCAction lcAction) {
      if (lcAction != null) {
        this.actions = new ArrayList<>();
        this.actions.add(lcAction);
      }
      return this;
    }

    public Builder addAction(OmLCAction lcAction) {
      if (lcAction != null) {
        this.actions.add(lcAction);
      }
      return this;
    }

    public Builder setActions(List<OmLCAction> lcAction) {
      if (lcAction != null) {
        this.actions = new ArrayList<>();
        this.actions.addAll(lcAction);
      }
      return this;
    }

    public Builder setFilter(OmLCFilter lcFilter) {
      this.filter = lcFilter;
      return this;
    }

    public OmLCFilter getFilter() {
      return filter;
    }

    public Builder setBucketLayout(BucketLayout layout) {
      this.bucketLayout = layout;
      return this;
    }

    public OmLCRule build() throws OMException {
      OmLCRule omLCRule = new OmLCRule(this);
      omLCRule.valid(bucketLayout);
      return omLCRule;
    }
  }
}
