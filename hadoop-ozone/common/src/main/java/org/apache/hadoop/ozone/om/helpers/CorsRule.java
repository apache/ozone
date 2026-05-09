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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CORSRule;

/**
 * One S3 bucket CORS rule.
 */
public final class CorsRule {
  private final String id;
  private final ImmutableList<String> allowedOrigins;
  private final ImmutableList<String> allowedMethods;
  private final ImmutableList<String> allowedHeaders;
  private final ImmutableList<String> exposeHeaders;
  private final Integer maxAgeSeconds;

  private CorsRule(Builder builder) {
    this.id = builder.id;
    this.allowedOrigins = ImmutableList.copyOf(builder.allowedOrigins);
    this.allowedMethods = ImmutableList.copyOf(builder.allowedMethods);
    this.allowedHeaders = ImmutableList.copyOf(builder.allowedHeaders);
    this.exposeHeaders = ImmutableList.copyOf(builder.exposeHeaders);
    this.maxAgeSeconds = builder.maxAgeSeconds;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public String getId() {
    return id;
  }

  public List<String> getAllowedOrigins() {
    return allowedOrigins;
  }

  public List<String> getAllowedMethods() {
    return allowedMethods;
  }

  public List<String> getAllowedHeaders() {
    return allowedHeaders;
  }

  public List<String> getExposeHeaders() {
    return exposeHeaders;
  }

  public Integer getMaxAgeSeconds() {
    return maxAgeSeconds;
  }

  public CORSRule getProtobuf() {
    CORSRule.Builder builder = CORSRule.newBuilder()
        .addAllAllowedOrigins(allowedOrigins)
        .addAllAllowedMethods(allowedMethods)
        .addAllAllowedHeaders(allowedHeaders)
        .addAllExposeHeaders(exposeHeaders);
    if (id != null) {
      builder.setId(id);
    }
    if (maxAgeSeconds != null) {
      builder.setMaxAgeSeconds(maxAgeSeconds);
    }
    return builder.build();
  }

  public static CorsRule getFromProtobuf(CORSRule proto) {
    Builder builder = newBuilder()
        .setAllowedOrigins(proto.getAllowedOriginsList())
        .setAllowedMethods(proto.getAllowedMethodsList())
        .setAllowedHeaders(proto.getAllowedHeadersList())
        .setExposeHeaders(proto.getExposeHeadersList());
    if (proto.hasId()) {
      builder.setId(proto.getId());
    }
    if (proto.hasMaxAgeSeconds()) {
      builder.setMaxAgeSeconds(proto.getMaxAgeSeconds());
    }
    return builder.build();
  }

  public Builder toBuilder() {
    return newBuilder()
        .setId(id)
        .setAllowedOrigins(allowedOrigins)
        .setAllowedMethods(allowedMethods)
        .setAllowedHeaders(allowedHeaders)
        .setExposeHeaders(exposeHeaders)
        .setMaxAgeSeconds(maxAgeSeconds);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof CorsRule)) {
      return false;
    }
    CorsRule that = (CorsRule) obj;
    return Objects.equals(id, that.id)
        && Objects.equals(allowedOrigins, that.allowedOrigins)
        && Objects.equals(allowedMethods, that.allowedMethods)
        && Objects.equals(allowedHeaders, that.allowedHeaders)
        && Objects.equals(exposeHeaders, that.exposeHeaders)
        && Objects.equals(maxAgeSeconds, that.maxAgeSeconds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, allowedOrigins, allowedMethods, allowedHeaders,
        exposeHeaders, maxAgeSeconds);
  }

  @Override
  public String toString() {
    return "CorsRule{"
        + "id='" + id + '\''
        + ", allowedOrigins=" + allowedOrigins
        + ", allowedMethods=" + allowedMethods
        + ", allowedHeaders=" + allowedHeaders
        + ", exposeHeaders=" + exposeHeaders
        + ", maxAgeSeconds=" + maxAgeSeconds
        + '}';
  }

  /**
   * Builder for {@link CorsRule}.
   */
  public static final class Builder {
    private String id;
    private List<String> allowedOrigins = ImmutableList.of();
    private List<String> allowedMethods = ImmutableList.of();
    private List<String> allowedHeaders = ImmutableList.of();
    private List<String> exposeHeaders = ImmutableList.of();
    private Integer maxAgeSeconds;

    private Builder() {
    }

    public Builder setId(String ruleId) {
      this.id = ruleId;
      return this;
    }

    public Builder setAllowedOrigins(List<String> origins) {
      this.allowedOrigins = origins == null ? ImmutableList.of() : origins;
      return this;
    }

    public Builder setAllowedMethods(List<String> methods) {
      this.allowedMethods = methods == null ? ImmutableList.of() : methods;
      return this;
    }

    public Builder setAllowedHeaders(List<String> headers) {
      this.allowedHeaders = headers == null ? ImmutableList.of() : headers;
      return this;
    }

    public Builder setExposeHeaders(List<String> headers) {
      this.exposeHeaders = headers == null ? ImmutableList.of() : headers;
      return this;
    }

    public Builder setMaxAgeSeconds(Integer seconds) {
      this.maxAgeSeconds = seconds;
      return this;
    }

    public CorsRule build() {
      return new CorsRule(this);
    }
  }
}
