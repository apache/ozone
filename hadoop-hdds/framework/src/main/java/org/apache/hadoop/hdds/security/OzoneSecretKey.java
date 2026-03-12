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

package org.apache.hadoop.hdds.security;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Objects;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;

/**
 * Wrapper class for Ozone/Hdds secret keys. Used in delegation tokens and block
 * tokens.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OzoneSecretKey {

  private int keyId;
  private long expiryDate;
  private PrivateKey privateKey;
  private PublicKey publicKey;
  private String certSerialId;

  public OzoneSecretKey(int keyId, long expiryDate, KeyPair keyPair,
      String certificateSerialId) {
    Objects.requireNonNull(keyPair, "keyPair == null");
    this.keyId = keyId;
    this.expiryDate = expiryDate;
    this.privateKey = keyPair.getPrivate();
    this.publicKey = keyPair.getPublic();
    this.certSerialId = certificateSerialId;
  }

  public int getKeyId() {
    return keyId;
  }

  public long getExpiryDate() {
    return expiryDate;
  }

  public PrivateKey getPrivateKey() {
    return privateKey;
  }

  public PublicKey getPublicKey() {
    return publicKey;
  }

  public String getCertSerialId() {
    return certSerialId;
  }

  public byte[] getEncodedPrivateKey() {
    return privateKey.getEncoded();
  }

  public byte[] getEncodedPubliceKey() {
    return publicKey.getEncoded();
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hashCodeBuilder = new HashCodeBuilder(537, 963);
    hashCodeBuilder.append(getExpiryDate())
        .append(getKeyId())
        .append(getEncodedPrivateKey())
        .append(getEncodedPubliceKey());

    return hashCodeBuilder.build();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }

    if (obj instanceof OzoneSecretKey) {
      OzoneSecretKey that = (OzoneSecretKey) obj;
      return new EqualsBuilder()
          .append(this.keyId, that.keyId)
          .append(this.expiryDate, that.expiryDate)
          .append(this.privateKey, that.privateKey)
          .append(this.publicKey, that.publicKey)
          .build();
    }
    return false;
  }

}
