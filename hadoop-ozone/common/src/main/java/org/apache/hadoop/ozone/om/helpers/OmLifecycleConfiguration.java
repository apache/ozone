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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import net.jcip.annotations.Immutable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CopyObject;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.Auditable;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleConfiguration;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleRule;

/**
 * A class that encapsulates lifecycle configuration.
 */
@Immutable
public final class OmLifecycleConfiguration extends WithObjectID
    implements Auditable, CopyObject<OmLifecycleConfiguration> {

  private static final Codec<OmLifecycleConfiguration> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(LifecycleConfiguration.getDefaultInstance()),
      OmLifecycleConfiguration::getFromProtobuf,
      OmLifecycleConfiguration::getProtobuf,
      OmLifecycleConfiguration.class);

  // Ref: https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html#intro-lifecycle-rule-id
  public static final int LC_MAX_RULES = 1000;
  private final String volume;
  private final String bucket;
  private final Long bucketObjectID;
  private final BucketLayout bucketLayout;
  private final long creationTime;
  private final List<OmLCRule> rules;

  public static Codec<OmLifecycleConfiguration> getCodec() {
    return CODEC;
  }

  private OmLifecycleConfiguration() {
    throw new UnsupportedOperationException("Default constructor is not supported. Use Builder.");
  }

  OmLifecycleConfiguration(OmLifecycleConfiguration.Builder builder) {
    super(builder);
    this.volume = builder.volume;
    this.bucket = builder.bucket;
    this.bucketObjectID = builder.bucketObjectID;
    this.rules = Collections.unmodifiableList(builder.rules);
    this.creationTime = builder.creationTime;
    this.bucketLayout = builder.bucketLayout;
  }

  public List<OmLCRule> getRules() {
    return rules;
  }

  public String getBucket() {
    return bucket;
  }

  public String getVolume() {
    return volume;
  }

  public Long getBucketObjectID() {
    return bucketObjectID;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }

  /**
   * Validates the lifecycle configuration.
   * - Volume and Bucket cannot be blank
   * - At least one rule needs to be specified
   * - Number of rules should not exceed the allowed limit
   * - Rules must have unique IDs
   * - Each rule is validated individually
   *
   * @throws OMException if the validation fails
   */
  public void valid() throws OMException {
    validateFields(volume, bucket, rules, bucketLayout, creationTime);
  }

  private static void validateFields(String volume, String bucket, List<OmLCRule> rules,
      BucketLayout bucketLayout, long creationTime) throws OMException {
    if (StringUtils.isBlank(volume)) {
      throw new OMException("Invalid lifecycle configuration: Volume cannot be blank.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    if (StringUtils.isBlank(bucket)) {
      throw new OMException("Invalid lifecycle configuration: Bucket cannot be blank.",
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

    if (!hasNoDuplicateID(rules)) {
      throw new OMException("Invalid lifecycle configuration: Duplicate rule IDs found.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    for (OmLCRule rule : rules) {
      rule.valid(bucketLayout, creationTime);
    }
  }

  private static boolean hasNoDuplicateID(List<OmLCRule> rules) {
    return rules.size() == rules.stream()
        .map(OmLCRule::getId)
        .collect(Collectors.toSet())
        .size();
  }

  public Builder toBuilder() {
    Builder builder = new Builder(this);
    builder.setVolume(this.volume)
        .setBucket(this.bucket)
        .setBucketLayout(bucketLayout)
        .setCreationTime(this.creationTime)
        .setRules(this.rules);
    if (bucketObjectID != null) {
      builder.setBucketObjectID(bucketObjectID);
    }
    return builder;
  }

  @Override
  public String toString() {
    return "OmLifecycleConfiguration{" +
        "volume='" + volume + '\'' +
        ", bucket='" + bucket + '\'' +
        ", bucketObjectID='" + bucketObjectID + '\'' +
        ", creationTime=" + creationTime +
        ", rulesCount=" + rules.size() +
        ", objectID=" + getObjectID() +
        ", updateID=" + getUpdateID() +
        '}';
  }

  @Override
  public Map<String, String> toAuditMap() {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, this.volume);
    auditMap.put(OzoneConsts.BUCKET, this.bucket);
    if (this.bucketObjectID != null) {
      auditMap.put(OzoneConsts.OBJECT_ID, String.valueOf(this.bucketObjectID));
    }
    auditMap.put(OzoneConsts.CREATION_TIME, String.valueOf(this.creationTime));

    return auditMap;
  }

  @Override
  public OmLifecycleConfiguration copyObject() {
    return toBuilder().buildObject();
  }

  public LifecycleConfiguration getProtobuf() {
    List<LifecycleRule> rulesProtoBuf = rules.stream()
        .map(OmLCRule::getProtobuf)
        .collect(Collectors.toList());

    LifecycleConfiguration.Builder b = LifecycleConfiguration.newBuilder()
        .setVolume(volume)
        .setBucket(bucket)
        .setBucketLayout(bucketLayout.toProto())
        .setCreationTime(creationTime)
        .addAllRules(rulesProtoBuf)
        .setObjectID(getObjectID())
        .setUpdateID(getUpdateID());

    if (bucketObjectID != null) {
      b.setBucketObjectID(bucketObjectID);
    }

    return b.build();
  }

  public static OmLifecycleConfiguration getFromProtobuf(
      LifecycleConfiguration lifecycleConfiguration) {
    return getBuilderFromProtobuf(lifecycleConfiguration).buildObject();
  }

  public static OmLifecycleConfiguration.Builder getBuilderFromProtobuf(
      LifecycleConfiguration lifecycleConfiguration) {
    List<OmLCRule> rulesList = new ArrayList<>();
    BucketLayout layout = BucketLayout.fromProto(lifecycleConfiguration.getBucketLayout());
    for (LifecycleRule lifecycleRule : lifecycleConfiguration.getRulesList()) {
      OmLCRule fromProtobuf = OmLCRule.getFromProtobuf(lifecycleRule, layout);
      rulesList.add(fromProtobuf);
    }

    Builder builder = new Builder()
        .setVolume(lifecycleConfiguration.getVolume())
        .setBucket(lifecycleConfiguration.getBucket())
        .setBucketLayout(layout)
        .setRules(rulesList);

    builder.setCreationTime(lifecycleConfiguration.getCreationTime());
    if (lifecycleConfiguration.hasObjectID()) {
      builder.setObjectID(lifecycleConfiguration.getObjectID());
    }
    if (lifecycleConfiguration.hasUpdateID()) {
      builder.setUpdateID(lifecycleConfiguration.getUpdateID());
    }
    if (lifecycleConfiguration.hasBucketObjectID()) {
      builder.setBucketObjectID(lifecycleConfiguration.getBucketObjectID());
    }

    return builder;
  }

  /**
   * Builder of OmLifecycleConfiguration.
   */
  public static class Builder extends WithObjectID.Builder<OmLifecycleConfiguration> {
    private String volume = "";
    private String bucket = "";
    private Long bucketObjectID;
    private BucketLayout bucketLayout;
    private long creationTime;
    private List<OmLCRule> rules = new ArrayList<>();

    private Builder(OmLifecycleConfiguration obj) {
      super(obj);
    }

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

    public Builder setBucketObjectID(long bucketID) {
      this.bucketObjectID = bucketID;
      return this;
    }

    public Builder setBucketLayout(BucketLayout layout) {
      this.bucketLayout = layout;
      return this;
    }

    public Builder setCreationTime(long creationTime) {
      this.creationTime = creationTime;
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

    @Override
    protected void validate() {
      super.validate();
      try {
        validateFields(volume, bucket, rules, bucketLayout, creationTime);
      } catch (OMException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    }

    @Override
    protected OmLifecycleConfiguration buildObject() {
      return new OmLifecycleConfiguration(this);
    }
  }
}
