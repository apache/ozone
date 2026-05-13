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

import java.util.Objects;
import net.jcip.annotations.Immutable;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleWithWebIdentityResponse;

/**
 * Utility class to handle AssumeRoleWithWebIdentityResponse protobuf message.
 */
@Immutable
public class AssumeRoleWithWebIdentityResponseInfo {

  private final String accessKeyId;
  private final String secretAccessKey;
  private final String sessionToken;
  private final long expirationEpochSeconds;
  private final String assumedRoleId;
  private final String subjectFromWebIdentityToken;
  private final String audience;
  private final String provider;

  public AssumeRoleWithWebIdentityResponseInfo(String accessKeyId,
      String secretAccessKey, String sessionToken, long expirationEpochSeconds,
      String assumedRoleId, String subjectFromWebIdentityToken, String audience,
      String provider) {
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.sessionToken = sessionToken;
    this.expirationEpochSeconds = expirationEpochSeconds;
    this.assumedRoleId = assumedRoleId;
    this.subjectFromWebIdentityToken = subjectFromWebIdentityToken;
    this.audience = audience;
    this.provider = provider;
  }

  public String getAccessKeyId() {
    return accessKeyId;
  }

  public String getSecretAccessKey() {
    return secretAccessKey;
  }

  public String getSessionToken() {
    return sessionToken;
  }

  public long getExpirationEpochSeconds() {
    return expirationEpochSeconds;
  }

  public String getAssumedRoleId() {
    return assumedRoleId;
  }

  public String getSubjectFromWebIdentityToken() {
    return subjectFromWebIdentityToken;
  }

  public String getAudience() {
    return audience;
  }

  public String getProvider() {
    return provider;
  }

  public static AssumeRoleWithWebIdentityResponseInfo fromProtobuf(
      AssumeRoleWithWebIdentityResponse response) {
    return new AssumeRoleWithWebIdentityResponseInfo(
        response.getAccessKeyId(), response.getSecretAccessKey(),
        response.getSessionToken(), response.getExpirationEpochSeconds(),
        response.getAssumedRoleId(),
        response.getSubjectFromWebIdentityToken(), response.getAudience(),
        response.hasProvider() ? response.getProvider() : null);
  }

  public AssumeRoleWithWebIdentityResponse getProtobuf() {
    AssumeRoleWithWebIdentityResponse.Builder builder =
        AssumeRoleWithWebIdentityResponse.newBuilder()
            .setAccessKeyId(accessKeyId)
            .setSecretAccessKey(secretAccessKey)
            .setSessionToken(sessionToken)
            .setExpirationEpochSeconds(expirationEpochSeconds)
            .setAssumedRoleId(assumedRoleId)
            .setSubjectFromWebIdentityToken(subjectFromWebIdentityToken)
            .setAudience(audience);
    if (provider != null && !provider.isEmpty()) {
      builder.setProvider(provider);
    }
    return builder.build();
  }

  @Override
  public String toString() {
    return "AssumeRoleWithWebIdentityResponseInfo{"
        + "accessKeyId='" + accessKeyId + '\''
        + ", secretAccessKey=<redacted>"
        + ", sessionToken=<redacted>"
        + ", expirationEpochSeconds=" + expirationEpochSeconds
        + ", assumedRoleId='" + assumedRoleId + '\''
        + ", subjectFromWebIdentityToken='" + subjectFromWebIdentityToken + '\''
        + ", audience='" + audience + '\''
        + ", provider='" + provider + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AssumeRoleWithWebIdentityResponseInfo that =
        (AssumeRoleWithWebIdentityResponseInfo) o;
    return expirationEpochSeconds == that.expirationEpochSeconds
        && Objects.equals(accessKeyId, that.accessKeyId)
        && Objects.equals(secretAccessKey, that.secretAccessKey)
        && Objects.equals(sessionToken, that.sessionToken)
        && Objects.equals(assumedRoleId, that.assumedRoleId)
        && Objects.equals(subjectFromWebIdentityToken,
            that.subjectFromWebIdentityToken)
        && Objects.equals(audience, that.audience)
        && Objects.equals(provider, that.provider);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accessKeyId, secretAccessKey, sessionToken,
        expirationEpochSeconds, assumedRoleId, subjectFromWebIdentityToken,
        audience, provider);
  }
}
