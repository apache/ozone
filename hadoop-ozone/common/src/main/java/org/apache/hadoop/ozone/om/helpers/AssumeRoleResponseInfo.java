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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AssumeRoleResponse;

/**
 * Utility class to handle AssumeRoleResponse protobuf message.
 */
@Immutable
public class AssumeRoleResponseInfo {

  private final String accessKeyId;
  private final String secretAccessKey;
  private final String sessionToken;
  private final long expirationEpochSeconds;
  private final String assumedRoleId;

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

  public AssumeRoleResponseInfo(
      String accessKeyId,
      String secretAccessKey,
      String sessionToken,
      long expirationEpochSeconds,
      String assumedRoleId
  ) {
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.sessionToken = sessionToken;
    this.expirationEpochSeconds = expirationEpochSeconds;
    this.assumedRoleId = assumedRoleId;
  }

  public static AssumeRoleResponseInfo fromProtobuf(
      AssumeRoleResponse response
  ) {
    return new AssumeRoleResponseInfo(
        response.getAccessKeyId(),
        response.getSecretAccessKey(),
        response.getSessionToken(),
        response.getExpirationEpochSeconds(),
        response.getAssumedRoleId()
    );
  }

  public AssumeRoleResponse getProtobuf() {
    return AssumeRoleResponse.newBuilder()
        .setAccessKeyId(accessKeyId)
        .setSecretAccessKey(secretAccessKey)
        .setSessionToken(sessionToken)
        .setExpirationEpochSeconds(expirationEpochSeconds)
        .setAssumedRoleId(assumedRoleId)
        .build();
  }

  @Override
  public String toString() {
    return "AssumeRoleResponseInfo{" +
        "accessKeyId='" + accessKeyId + '\'' +
        ", secretAccessKey='" + secretAccessKey + '\'' +
        ", sessionToken='" + sessionToken + '\'' +
        ", expirationEpochSeconds=" + expirationEpochSeconds +
        ", assumedRoleId='" + assumedRoleId + '\'' +
        '}';
  }

  @Override
  public boolean equals(
      Object o
  ) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AssumeRoleResponseInfo that = (AssumeRoleResponseInfo) o;
    return expirationEpochSeconds == that.expirationEpochSeconds &&
        Objects.equals(accessKeyId, that.accessKeyId) &&
        Objects.equals(secretAccessKey, that.secretAccessKey) &&
        Objects.equals(sessionToken, that.sessionToken) &&
        Objects.equals(assumedRoleId, that.assumedRoleId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        accessKeyId,
        secretAccessKey,
        sessionToken,
        expirationEpochSeconds,
        assumedRoleId
    );
  }
}
