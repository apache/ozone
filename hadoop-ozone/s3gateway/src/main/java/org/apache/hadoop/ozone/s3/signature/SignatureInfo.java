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

import javax.enterprise.context.RequestScoped;

/**
 * Signature and related information.
 * <p>
 * Required to create stringToSign and token.
 */
@RequestScoped
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

  private String unfilteredURI = null;


  private String stringToSign = null;

  public SignatureInfo() { }

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
    initialize(version, date, dateTime, awsAccessId, signature, signedHeaders,
        credentialScope, algorithm, signPayload, null, null);
  }

  public void initialize(
      SignatureInfo signatureInfo
  ) {
    initialize(signatureInfo.getVersion(), signatureInfo.getDate(),
        signatureInfo.getDateTime(), signatureInfo.getAwsAccessId(),
        signatureInfo.getSignature(), signatureInfo.getSignedHeaders(),
        signatureInfo.getCredentialScope(), signatureInfo.getAlgorithm(),
        signatureInfo.isSignPayload(), signatureInfo.getUnfilteredURI(),
        signatureInfo.getStringToSign());
  }

  @SuppressWarnings({"checkstyle:ParameterNumber", "checkstyle:HiddenField"})
  public void initialize(
      Version version,
      String date,
      String dateTime,
      String awsAccessId,
      String signature,
      String signedHeaders,
      String credentialScope,
      String algorithm,
      boolean signPayload,
      String uri,
      String stringToSign
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
    this.unfilteredURI = uri;
    this.stringToSign = stringToSign;
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

  public String getUnfilteredURI() {
    return unfilteredURI;
  }

  public String getStringToSign() {
    return stringToSign;
  }

  public void setUnfilteredURI(String uri) {
    this.unfilteredURI = uri;
  }

  public void setStrToSign(String strToSign) {
    this.stringToSign = strToSign;
  }

  /**
   * Signature version.
   */
  public enum Version {
    NONE, V4, V2;
  }
}
