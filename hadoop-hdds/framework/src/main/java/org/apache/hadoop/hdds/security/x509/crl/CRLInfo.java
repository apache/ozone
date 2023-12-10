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

package org.apache.hadoop.hdds.security.x509.crl;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.CRLInfoProto;
import org.apache.hadoop.hdds.protocol.scm.proto.SCMUpdateServiceProtos;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.jetbrains.annotations.NotNull;

import java.security.cert.CRLException;
import java.security.cert.X509CRL;
import java.security.cert.X509CRLEntry;
import java.time.Instant;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.Function;

/**
 * Class that wraps Certificate Revocation List Info.
 */
public final class CRLInfo implements Comparator<CRLInfo>,
    Comparable<CRLInfo> {

  private static final Codec<CRLInfo> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(CRLInfoProto.getDefaultInstance()),
      proto -> fromProtobuf(proto, CRLCodec::toIOException),
      CRLInfo::getProtobuf);

  public static Codec<CRLInfo> getCodec() {
    return CODEC;
  }

  private final X509CRL x509CRL;
  private final long creationTimestamp;
  private final long crlSequenceID;
  private final Instant revocationTime;

  private CRLInfo(X509CRL x509CRL, long creationTimestamp, long crlSequenceID) {
    assert ((x509CRL != null) &&
        !x509CRL.getRevokedCertificates().isEmpty());
    this.x509CRL = x509CRL;
    this.creationTimestamp = creationTimestamp;
    this.crlSequenceID = crlSequenceID;
    X509CRLEntry entry = x509CRL.getRevokedCertificates().iterator().next();
    this.revocationTime = Instant.ofEpochMilli(
        entry.getRevocationDate().getTime());
  }

  public static CRLInfo fromProtobuf(CRLInfoProto info)
      throws CRLException {
    return fromProtobuf(info, Function.identity());
  }

  private static <E extends Exception> CRLInfo fromProtobuf(
      CRLInfoProto info, Function<CRLException, E> convertor) throws E {
    return new CRLInfo.Builder()
        .setX509CRL(CRLCodec.getX509CRL(info.getX509CRL(), convertor))
        .setCreationTimestamp(info.getCreationTimestamp())
        .setCrlSequenceID(info.getCrlSequenceID())
        .build();
  }

  public CRLInfoProto getProtobuf() throws SCMSecurityException {
    return CRLInfoProto.newBuilder()
        .setX509CRL(CRLCodec.getPEMEncodedString(getX509CRL()))
        .setCreationTimestamp(getCreationTimestamp())
        .setCrlSequenceID(getCrlSequenceID())
        .build();
  }

  public static CRLInfo fromCRLProto3(
      SCMUpdateServiceProtos.CRLInfoProto info)
      throws CRLException {
    return new CRLInfo.Builder()
        .setX509CRL(CRLCodec.getX509CRL(info.getX509CRL()))
        .setCreationTimestamp(info.getCreationTimestamp())
        .setCrlSequenceID(info.getCrlSequenceID())
        .build();
  }

  public SCMUpdateServiceProtos.CRLInfoProto getCRLProto3()
      throws SCMSecurityException {
    return SCMUpdateServiceProtos.CRLInfoProto.newBuilder()
        .setX509CRL(CRLCodec.getPEMEncodedString(getX509CRL()))
        .setCreationTimestamp(getCreationTimestamp())
        .setCrlSequenceID(getCrlSequenceID())
        .build();
  }

  public X509CRL getX509CRL() {
    return x509CRL;
  }

  public long getCreationTimestamp() {
    return creationTimestamp;
  }

  public long getCrlSequenceID() {
    return crlSequenceID;
  }

  public boolean shouldRevokeNow() {
    return revocationTime.isBefore(Instant.now());
  }

  public Instant getRevocationTime() {
    return revocationTime;
  }

  /**
   * Compares this object with the specified object for order.  Returns a
   * negative integer, zero, or a positive integer as this object is less
   * than, equal to, or greater than the specified object.
   *
   * @param o the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object
   * is less than, equal to, or greater than the specified object.
   * @throws NullPointerException if the specified object is null
   * @throws ClassCastException   if the specified object's type prevents it
   *                              from being compared to this object.
   */
  @Override
  public int compareTo(@NotNull CRLInfo o) {
    return this.compare(this, o);
  }

  /**
   * Compares its two arguments for order.  Returns a negative integer,
   * zero, or a positive integer as the first argument is less than, equal
   * to, or greater than the second.<p>
   * <p>
   *
   * @param o1 the first object to be compared.
   * @param o2 the second object to be compared.
   * @return a negative integer, zero, or a positive integer as the
   * first argument is less than, equal to, or greater than the
   * second.
   * @throws NullPointerException if an argument is null and this
   *                              comparator does not permit null arguments
   * @throws ClassCastException   if the arguments' types prevent them from
   *                              being compared by this comparator.
   */
  @Override
  public int compare(CRLInfo o1, CRLInfo o2) {
    return Long.compare(o1.getCreationTimestamp(), o2.getCreationTimestamp());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CRLInfo that = (CRLInfo) o;

    return this.crlSequenceID == that.crlSequenceID &&
        this.getX509CRL().equals(that.x509CRL) &&
        this.creationTimestamp == that.creationTimestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getX509CRL(), getCreationTimestamp());
  }

  @Override
  public String toString() {
    return "CRLInfo{" +
        "crlSequenceID=" + crlSequenceID +
        ", x509CRL=" + x509CRL.toString() +
        ", creationTimestamp=" + creationTimestamp +
        '}';
  }

  /**
   * Builder class for CRLInfo.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static class Builder {
    private X509CRL x509CRL;
    private long creationTimestamp;
    private long crlSequenceID;

    public Builder setX509CRL(X509CRL x509CRL) {
      this.x509CRL = x509CRL;
      return this;
    }

    public Builder setCreationTimestamp(long creationTimestamp) {
      this.creationTimestamp = creationTimestamp;
      return this;
    }

    public Builder setCrlSequenceID(long crlSequenceID) {
      this.crlSequenceID = crlSequenceID;
      return this;
    }

    public CRLInfo build() {
      return new CRLInfo(x509CRL, creationTimestamp, crlSequenceID);
    }
  }
}
