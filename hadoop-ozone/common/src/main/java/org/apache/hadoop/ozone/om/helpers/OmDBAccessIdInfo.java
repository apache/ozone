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
   * Kerberos principal this accessId belongs to.
   */
  private final String kerberosPrincipal;
  /**
   * Shared secret of the accessId. TODO: Encryption?
   */
  private final String sharedSecret;

  // This implies above String fields should NOT contain the split key.
  public static final String SERIALIZATION_SPLIT_KEY = ";";

  public OmDBAccessIdInfo(String tenantId,
      String kerberosPrincipal, String sharedSecret) {
    this.tenantId = tenantId;
    this.kerberosPrincipal = kerberosPrincipal;
    this.sharedSecret = sharedSecret;
  }

  private OmDBAccessIdInfo(String accessIdInfoString) {
    String[] tInfo = accessIdInfoString.split(SERIALIZATION_SPLIT_KEY);
    Preconditions.checkState(tInfo.length == 3,
        "Incorrect accessIdInfoString");

    tenantId = tInfo[0];
    kerberosPrincipal = tInfo[1];
    sharedSecret = tInfo[2];
  }

  public String getTenantId() {
    return tenantId;
  }

  private String serialize() {
    StringBuilder sb = new StringBuilder();
    sb.append(tenantId).append(SERIALIZATION_SPLIT_KEY);
    sb.append(kerberosPrincipal).append(SERIALIZATION_SPLIT_KEY);
    sb.append(sharedSecret);
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

  public String getKerberosPrincipal() {
    return kerberosPrincipal;
  }

  public String getSharedSecret() {
    return sharedSecret;
  }

  /**
   * Builder for OmDBAccessIdInfo.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private String tenantId;
    private String kerberosPrincipal;
    private String sharedSecret;

    public Builder setTenantName(String tenantId) {
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

    public OmDBAccessIdInfo build() {
      return new OmDBAccessIdInfo(tenantId, kerberosPrincipal, sharedSecret);
    }
  }
}
