/**
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
package org.apache.hadoop.ozone.s3.signature;

/**
 * Signature and related information.
 * <p>
 * Required to create stringToSign and token.
 */
public class SignatureInfo {

  private Version version;

  /**
   * Information comes from the credential (Date only).
   */
  private String date;

  /**
   * Information comes from header/query param (full timestamp).
   */
  private String dateTime;

  private String awsAccessId;

  private String signature;

  private String signedHeaders;

  private String credentialScope;

  private String algorithm;

  private boolean signPayload = true;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public SignatureInfo(
      Version version,
      String date,
      String dateTime,
      String awsAccessId,
      String signature,
      String signedHeaders,
      String credentialScope,
      String algorithm,
      boolean signPayload
  ) {
    this.version = version;
    this.date = date;
    this.dateTime = dateTime;
    this.awsAccessId = awsAccessId;
    this.signature = signature;
    this.signedHeaders = signedHeaders;
    this.credentialScope = credentialScope;
    this.algorithm = algorithm;
    this.signPayload = signPayload;
  }

  public String getAwsAccessId() {
    return awsAccessId;
  }

  public String getSignature() {
    return signature;
  }

  public String getDate() {
    return date;
  }

  public String getSignedHeaders() {
    return signedHeaders;
  }

  public String getCredentialScope() {
    return credentialScope;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public Version getVersion() {
    return version;
  }

  public boolean isSignPayload() {
    return signPayload;
  }

  public String getDateTime() {
    return dateTime;
  }

  /**
   * Signature version.
   */
  public enum Version {
    NONE, V4, V2;
  }
}
