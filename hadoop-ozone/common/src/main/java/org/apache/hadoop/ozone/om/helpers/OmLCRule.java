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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;

/**
 * A class that encapsulates lifecycle rule.
 */
public class OmLCRule {

  public static final int LC_ID_LENGTH = 48;
  // Ref: https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html#intro-lifecycle-rule-id
  public static final int LC_ID_MAX_LENGTH = 255;

  private String id;
  private String prefix;
  private boolean enabled;
  private boolean isPrefixEnable;
  private boolean isTagEnable;
  // List of actions for this rule
  private List<OmLCAction> actions;
  private OmLCFilter filter;

  OmLCRule(String id, String prefix, boolean enabled,
      List<OmLCAction> actions, OmLCFilter filter) {
    this.id = id;
    this.prefix = prefix;
    this.enabled = enabled;
    this.actions = actions;
    this.filter = filter;

    // If no ID is specified in the lifecycle configure, a random ID will be generated
    if (StringUtils.isEmpty(this.id)) {
      this.id = RandomStringUtils.randomAlphanumeric(LC_ID_LENGTH);
    }

    OmLifecycleRuleAndOperator andOperator = filter != null ? filter.getAndOperator() : null;

    if (prefix != null ||
        (filter != null && filter.getPrefix() != null) ||
        (andOperator != null && andOperator.getPrefix() != null)) {
      isPrefixEnable = true;
    }

    if ((filter != null && filter.getTag() != null) ||
        (andOperator != null && !andOperator.getTags().isEmpty())) {
      isTagEnable = true;
    }
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public List<OmLCAction> getActions() {
    return actions;
  }

  public void setActions(List<OmLCAction> actions) {
    this.actions = actions;
  }

  /**
   * Get the expiration action if present.
   *
   * @return the expiration action if present, null otherwise
   */
  public OmLCExpiration getExpiration() {
    if (actions == null || actions.isEmpty()) {
      return null;
    }

    for (OmLCAction action : actions) {
      if (action instanceof OmLCExpiration) {
        return (OmLCExpiration) action;
      }
    }
    return null;
  }

  public OmLCFilter getFilter() {
    return filter;
  }

  public boolean isPrefixEnable() {
    return isPrefixEnable;
  }

  public boolean isTagEnable() {
    return isTagEnable;
  }

  public void setFilter(OmLCFilter filter) {
    this.filter = filter;
  }

  /**
   * Validates the lifecycle rule.
   * - ID length should not exceed the allowed limit
   * - At least one action must be specified
   * - Filter and Prefix cannot be used together
   * - Actions must be valid
   * - Filter must be valid
   * - There must be at most one Expiration action per rule
   *
   * @throws OMException if the validation fails
   */
  public void valid() throws OMException {
    if (id.length() > LC_ID_MAX_LENGTH) {
      throw new OMException("ID length should not exceed allowed limit of " + LC_ID_MAX_LENGTH,
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (actions == null || actions.isEmpty()) {
      throw new OMException("At least one action needs to be specified in a rule.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    // Check that there is at most one Expiration action
    for (OmLCAction action : actions) {
      if (action.getActionType() == OmLCAction.ActionType.EXPIRATION) {
        if (actions.size() > 1) {
          throw new OMException("A rule can have at most one Expiration action.",
              OMException.ResultCodes.INVALID_REQUEST);
        }
      }
      action.valid();
    }

    if (prefix != null && filter != null) {
      throw new OMException("Filter and Prefix cannot be used together.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (filter != null) {
      filter.valid();
    }
  }

  @Override
  public String toString() {
    return "OmLCRule{" +
        "id='" + id + '\'' +
        ", prefix='" + prefix + '\'' +
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
    private boolean enabled;
    private List<OmLCAction> actions = new ArrayList<>();
    private OmLCFilter filter;

    public Builder setId(String lcId) {
      this.id = lcId;
      return this;
    }

    public Builder setPrefix(String lcPrefix) {
      this.prefix = lcPrefix;
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

    public Builder setFilter(OmLCFilter lcFilter) {
      this.filter = lcFilter;
      return this;
    }

    public OmLCFilter getFilter() {
      return filter;
    }

    public OmLCRule build() {
      return new OmLCRule(id, prefix, enabled, actions, filter);
    }
  }
}
