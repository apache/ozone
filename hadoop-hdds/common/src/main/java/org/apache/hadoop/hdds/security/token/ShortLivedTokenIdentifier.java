/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.security.token;

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.time.Instant;
import java.util.Objects;

/**
 * Base class for short-lived tokens (block, container).
 */
@InterfaceAudience.Private
public abstract class ShortLivedTokenIdentifier extends TokenIdentifier {

  private String ownerId;
  private Instant expiry;
  private String certSerialId;

  public abstract String getService();

  protected ShortLivedTokenIdentifier() {
  }

  protected ShortLivedTokenIdentifier(String ownerId, Instant expiry,
      String certSerialId) {
    this.ownerId = ownerId;
    this.expiry = expiry;
    this.certSerialId = certSerialId;
  }

  @Override
  public UserGroupInformation getUser() {
    if (Strings.isNullOrEmpty(this.getOwnerId())) {
      return UserGroupInformation.createRemoteUser(getService());
    }
    return UserGroupInformation.createRemoteUser(ownerId);
  }

  public boolean isExpired(Instant at) {
    return expiry.isBefore(at);
  }

  protected void setOwnerId(String ownerId) {
    this.ownerId = ownerId;
  }

  protected void setExpiry(Instant expiry) {
    this.expiry = expiry;
  }

  protected void setCertSerialId(String certSerialId) {
    this.certSerialId = certSerialId;
  }

  public Instant getExpiry() {
    return expiry;
  }

  public String getCertSerialId() {
    return certSerialId;
  }

  public String getOwnerId() {
    return ownerId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ShortLivedTokenIdentifier that = (ShortLivedTokenIdentifier) o;
    return Objects.equals(ownerId, that.ownerId) &&
        Objects.equals(expiry, that.expiry) &&
        Objects.equals(certSerialId, that.certSerialId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ownerId, expiry, certSerialId);
  }

  @Override
  public String toString() {
    return "ownerId=" + ownerId +
        ", expiry=" + expiry +
        ", certSerialId=" + certSerialId;
  }
}
