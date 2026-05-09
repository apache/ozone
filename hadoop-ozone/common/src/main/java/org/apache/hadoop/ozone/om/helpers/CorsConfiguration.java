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

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CORSConfiguration;

/**
 * S3 bucket CORS configuration.
 */
public final class CorsConfiguration {
  private final ImmutableList<CorsRule> rules;

  private CorsConfiguration(Builder builder) {
    this.rules = ImmutableList.copyOf(builder.rules);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public List<CorsRule> getRules() {
    return rules;
  }

  public CORSConfiguration getProtobuf() {
    return CORSConfiguration.newBuilder()
        .addAllCorsRule(rules.stream()
            .map(CorsRule::getProtobuf)
            .collect(Collectors.toList()))
        .build();
  }

  public static CorsConfiguration getFromProtobuf(
      CORSConfiguration proto) {
    return newBuilder()
        .setRules(proto.getCorsRuleList().stream()
            .map(CorsRule::getFromProtobuf)
            .collect(Collectors.toList()))
        .build();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof CorsConfiguration)) {
      return false;
    }
    CorsConfiguration that = (CorsConfiguration) obj;
    return Objects.equals(rules, that.rules);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rules);
  }

  @Override
  public String toString() {
    return "CorsConfiguration{"
        + "rules=" + rules
        + '}';
  }

  /**
   * Builder for {@link CorsConfiguration}.
   */
  public static final class Builder {
    private List<CorsRule> rules = ImmutableList.of();

    private Builder() {
    }

    public Builder setRules(List<CorsRule> corsRules) {
      this.rules = corsRules == null ? ImmutableList.of() : corsRules;
      return this;
    }

    public Builder addRule(CorsRule rule) {
      ImmutableList.Builder<CorsRule> builder = ImmutableList.builder();
      builder.addAll(rules);
      builder.add(rule);
      this.rules = builder.build();
      return this;
    }

    public CorsConfiguration build() {
      return new CorsConfiguration(this);
    }
  }
}
