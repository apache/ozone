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
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Secret;

/**
 * S3Secret to be saved in database.
 */
public final class S3SecretValue {
  private static final Codec<S3SecretValue> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(S3Secret.getDefaultInstance()),
      S3SecretValue::fromProtobuf,
      S3SecretValue::getProtobuf,
      S3SecretValue.class);

  // TODO: This field should be renamed to accessId for generalization.
  private final String kerberosID;
  private final String awsSecret;
  private final boolean isDeleted;
  private final long transactionLogIndex;

  public static Codec<S3SecretValue> getCodec() {
    return CODEC;
  }

  public static S3SecretValue of(String kerberosID, String awsSecret) {
    return of(kerberosID, awsSecret, 0);
  }

  public static S3SecretValue of(String kerberosID, String awsSecret, long transactionLogIndex) {
    return new S3SecretValue(
        Objects.requireNonNull(kerberosID),
        Objects.requireNonNull(awsSecret),
        false,
        transactionLogIndex
    );
  }

  public S3SecretValue deleted() {
    return new S3SecretValue(kerberosID, "", true, transactionLogIndex);
  }

  private S3SecretValue(String kerberosID, String awsSecret, boolean isDeleted,
                       long transactionLogIndex) {
    this.kerberosID = kerberosID;
    this.awsSecret = awsSecret;
    this.isDeleted = isDeleted;
    this.transactionLogIndex = transactionLogIndex;
  }

  public String getKerberosID() {
    return kerberosID;
  }

  public String getAwsSecret() {
    return awsSecret;
  }

  public boolean isDeleted() {
    return isDeleted;
  }

  public String getAwsAccessKey() {
    return kerberosID;
  }

  public long getTransactionLogIndex() {
    return transactionLogIndex;
  }

  public static S3SecretValue fromProtobuf(S3Secret s3Secret) {
    return S3SecretValue.of(s3Secret.getKerberosID(), s3Secret.getAwsSecret());
  }

  public S3Secret getProtobuf() {
    return S3Secret.newBuilder()
        .setAwsSecret(this.awsSecret)
        .setKerberosID(this.kerberosID)
        .build();
  }

  @Override
  public String toString() {
    return "awsAccessKey=" + kerberosID + "\nawsSecret=" + awsSecret;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    S3SecretValue that = (S3SecretValue) o;
    return kerberosID.equals(that.kerberosID) &&
        awsSecret.equals(that.awsSecret) && isDeleted == that.isDeleted &&
        transactionLogIndex == that.transactionLogIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(kerberosID, awsSecret, isDeleted, transactionLogIndex);
  }
}
