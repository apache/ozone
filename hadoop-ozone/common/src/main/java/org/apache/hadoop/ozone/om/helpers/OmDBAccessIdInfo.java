/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.StringUtils;

/**
 * This class is used for storing Ozone tenant accessId info.
 */
public final class OmDBAccessIdInfo {
  /**
   * Name of the tenant.
   */
  private final String tenantId;
  /**
   * User principal this accessId belongs to.
   */
  private final String userPrincipal;
  /**
   * Corresponding secret key for the accessId.
   */
  private final String secretKey;
  /**
   * Whether this accessId is an administrator of the tenant.
   */
  private final boolean isAdmin;
  /**
   * Whether this accessId is a delegated admin of the tenant.
   * Only effective if isAdmin is true.
   */
  private final boolean isDelegatedAdmin;

  // This implies above String fields should NOT contain the split key.
  public static final String SERIALIZATION_SPLIT_KEY = ";";

  public OmDBAccessIdInfo(String tenantId,
                          String userPrincipal, String secretKey,
                          boolean isAdmin, boolean isDelegatedAdmin) {
    this.tenantId = tenantId;
    this.userPrincipal = userPrincipal;
    this.secretKey = secretKey;
    this.isAdmin = isAdmin;
    this.isDelegatedAdmin = isDelegatedAdmin;
  }

  private OmDBAccessIdInfo(String accessIdInfoString) {
    String[] tInfo = accessIdInfoString.split(SERIALIZATION_SPLIT_KEY);
    Preconditions.checkState(tInfo.length == 3 || tInfo.length == 5,
        "Incorrect accessIdInfoString");

    tenantId = tInfo[0];
    userPrincipal = tInfo[1];
    secretKey = tInfo[2];
    if (tInfo.length == 5) {
      isAdmin = Boolean.parseBoolean(tInfo[3]);
      isDelegatedAdmin = Boolean.parseBoolean(tInfo[4]);
    } else {
      isAdmin = false;
      isDelegatedAdmin = false;
    }
  }

  public String getTenantId() {
    return tenantId;
  }

  private String serialize() {
    final StringBuilder sb = new StringBuilder();
    sb.append(tenantId);
    sb.append(SERIALIZATION_SPLIT_KEY).append(userPrincipal);
    sb.append(SERIALIZATION_SPLIT_KEY).append(secretKey);
    sb.append(SERIALIZATION_SPLIT_KEY).append(isAdmin);
    sb.append(SERIALIZATION_SPLIT_KEY).append(isDelegatedAdmin);
    return sb.toString();
  }

  /**
   * Convert OmDBAccessIdInfo to byteArray to be persisted to DB.
   * @return byte[]
   */
  public byte[] convertToByteArray() {
    return StringUtils.string2Bytes(serialize());
  }

  /**
   * Convert byte array to OmDBAccessIdInfo.
   */
  public static OmDBAccessIdInfo getFromByteArray(byte[] bytes) {
    String tInfo = StringUtils.bytes2String(bytes);
    return new OmDBAccessIdInfo(tInfo);
  }

  public String getUserPrincipal() {
    return userPrincipal;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public boolean getIsAdmin() {
    return isAdmin;
  }

  public boolean getIsDelegatedAdmin() {
    return isDelegatedAdmin;
  }

  /**
   * Builder for OmDBAccessIdInfo.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private String tenantId;
    private String kerberosPrincipal;
    private String sharedSecret;
    private boolean isAdmin;
    private boolean isDelegatedAdmin;

    public Builder setTenantId(String tenantId) {
      this.tenantId = tenantId;
      return this;
    }

    public Builder setKerberosPrincipal(String kerberosPrincipal) {
      this.kerberosPrincipal = kerberosPrincipal;
      return this;
    }

    public Builder setSharedSecret(String sharedSecret) {
      this.sharedSecret = sharedSecret;
      return this;
    }

    public Builder setIsAdmin(boolean isAdmin) {
      this.isAdmin = isAdmin;
      return this;
    }

    public Builder setIsDelegatedAdmin(boolean isDelegatedAdmin) {
      this.isDelegatedAdmin = isDelegatedAdmin;
      return this;
    }

    public OmDBAccessIdInfo build() {
      return new OmDBAccessIdInfo(tenantId, kerberosPrincipal, sharedSecret,
          isAdmin, isDelegatedAdmin);
    }
  }
}
