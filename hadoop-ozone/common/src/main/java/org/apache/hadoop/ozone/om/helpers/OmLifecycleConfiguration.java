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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.om.exceptions.OMException;

/**
 * A class that encapsulates lifecycle configuration.
 */
public class OmLifecycleConfiguration extends WithObjectID
    implements Auditable {

  // Ref: https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html#intro-lifecycle-rule-id
  public static final int LC_MAX_RULES = 1000;
  private String volume;
  private String bucket;
  private String owner;
  private long creationTime;
  private List<OmLCRule> rules;

  OmLifecycleConfiguration(OmLifecycleConfiguration.Builder builder) {
    super(builder);
    this.volume = builder.volume;
    this.bucket = builder.bucket;
    this.owner = builder.owner;
    this.rules = builder.rules;
    this.creationTime = builder.creationTime;
  }

  public List<OmLCRule> getRules() {
    return rules;
  }

  public void setRules(List<OmLCRule> rules) {
    this.rules = rules;
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public String getVolume() {
    return volume;
  }

  public void setVolume(String volume) {
    this.volume = volume;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(long creationTime) {
    this.creationTime = creationTime;
  }

  /**
   * Validates the lifecycle configuration.
   * - Volume, Bucket and Owner cannot be blank
   * - At least one rule needs to be specified
   * - Number of rules should not exceed the allowed limit
   * - Rules must have unique IDs
   * - Each rule is validated individually
   *
   * @throws OMException if the validation fails
   */
  public void valid() throws OMException {
    if (StringUtils.isBlank(volume)) {
      throw new OMException("Invalid lifecycle configuration: Volume cannot be blank.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (StringUtils.isBlank(bucket)) {
      throw new OMException("Invalid lifecycle configuration: Bucket cannot be blank.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (StringUtils.isBlank(owner)) {
      throw new OMException("Invalid lifecycle configuration: Owner cannot be blank.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (rules.isEmpty()) {
      throw new OMException("At least one rules needs to be specified in a lifecycle configuration.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (rules.size() > LC_MAX_RULES) {
      throw new OMException("The number of lifecycle rules must not exceed the allowed limit of "
          + LC_MAX_RULES + " rules", OMException.ResultCodes.INVALID_REQUEST);
    }

    if (!hasNoDuplicateID()) {
      throw new OMException("Invalid lifecycle configuration: Duplicate rule IDs found.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    for (OmLCRule rule : rules) {
      rule.valid();
    }
  }

  private boolean hasNoDuplicateID() {
    return rules.size() == rules.stream()
        .map(OmLCRule::getId)
        .collect(Collectors.toSet())
        .size();
  }

  public Builder toBuilder() {
    return new Builder()
        .setVolume(volume)
        .setBucket(this.bucket)
        .setOwner(this.owner)
        .setCreationTime(this.creationTime)
        .setRules(this.rules)
        .setUpdateID(super.getUpdateID())
        .setObjectID(super.getObjectID());
  }

  @Override
  public String toString() {
    return "OmLifecycleConfiguration{" +
        "volume='" + volume + '\'' +
        ", bucket='" + bucket + '\'' +
        ", owner='" + owner + '\'' +
        ", creationTime=" + creationTime +
        ", rulesCount=" + rules.size() +
        ", objectID=" + getObjectID() +
        ", updateID=" + getUpdateID() +
        '}';
  }
  /**
   * Returns formatted key to be used as prevKey when listing lifecycle
   * configurations.
   *
   * @return volume/bucket
   */
  public String getFormattedKey() {
    return volume + "/" + bucket;
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, this.volume);
    auditMap.put(OzoneConsts.BUCKET, this.bucket);
    auditMap.put(OzoneConsts.OWNER, this.owner);
    auditMap.put(OzoneConsts.CREATION_TIME, String.valueOf(this.creationTime));

    return auditMap;
  }

  /**
   * Builder of OmLifecycleConfiguration.
   */
  public static class Builder extends WithObjectID.Builder {
    private String volume = "";
    private String bucket = "";
    private String owner = "";
    private long creationTime;
    private List<OmLCRule> rules = new ArrayList<>();

    public Builder() {
    }

    public Builder setVolume(String volumeName) {
      this.volume = volumeName;
      return this;
    }

    public Builder setBucket(String bucketName) {
      this.bucket = bucketName;
      return this;
    }

    public Builder setOwner(String ownerName) {
      this.owner = ownerName;
      return this;
    }

    public Builder setCreationTime(long ctime) {
      this.creationTime = ctime;
      return this;
    }

    public Builder addRule(OmLCRule rule) {
      this.rules.add(rule);
      return this;
    }

    public Builder setRules(List<OmLCRule> lcRules) {
      this.rules = lcRules;
      return this;
    }

    @Override
    public Builder setObjectID(long oID) {
      super.setObjectID(oID);
      return this;
    }

    @Override
    public Builder setUpdateID(long uID) {
      super.setUpdateID(uID);
      return this;
    }

    public OmLifecycleConfiguration build() {
      return new OmLifecycleConfiguration(this);
    }
  }
}
