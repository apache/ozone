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

  private SignatureInfo(Builder b) {
    initialize(b);
  }

  public void initialize(SignatureInfo signatureInfo) {
    initialize(new Builder(signatureInfo.getVersion())
        .setDate(signatureInfo.getDate())
        .setDateTime(signatureInfo.getDateTime())
        .setAwsAccessId(signatureInfo.getAwsAccessId())
        .setSignature(signatureInfo.getSignature())
        .setSignedHeaders(signatureInfo.getSignedHeaders())
        .setCredentialScope(signatureInfo.getCredentialScope())
        .setAlgorithm(signatureInfo.getAlgorithm())
        .setSignPayload(signatureInfo.isSignPayload())
        .setUnfilteredURI(signatureInfo.getUnfilteredURI())
        .setStringToSign(signatureInfo.getStringToSign()));
  }

  private void initialize(Builder b) {
    this.version = b.version;
    this.date = b.date;
    this.dateTime = b.dateTime;
    this.awsAccessId = b.awsAccessId;
    this.signature = b.signature;
    this.signedHeaders = b.signedHeaders;
    this.credentialScope = b.credentialScope;
    this.algorithm = b.algorithm;
    this.signPayload = b.signPayload;
    this.unfilteredURI = b.unfilteredURI;
    this.stringToSign = b.stringToSign;
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

  /**
   * Builder class for SignatureInfo.
   */
  public static class Builder {
    private Version version;
    private String date = "";
    private String dateTime = "";
    private String awsAccessId = "";
    private String signature = "";
    private String signedHeaders = "";
    private String credentialScope = "";
    private String algorithm = "";
    private boolean signPayload = true;
    private String unfilteredURI = null;
    private String stringToSign = null;

    public Builder(Version version) {
      this.version = version;
    }

    public Builder setDate(String date) {
      this.date = date;
      return this;
    }

    public Builder setDateTime(String dateTime) {
      this.dateTime = dateTime;
      return this;
    }

    public Builder setAwsAccessId(String awsAccessId) {
      this.awsAccessId = awsAccessId;
      return this;
    }

    public Builder setSignature(String signature) {
      this.signature = signature;
      return this;
    }

    public Builder setSignedHeaders(String signedHeaders) {
      this.signedHeaders = signedHeaders;
      return this;
    }

    public Builder setCredentialScope(String credentialScope) {
      this.credentialScope = credentialScope;
      return this;
    }

    public Builder setAlgorithm(String algorithm) {
      this.algorithm = algorithm;
      return this;
    }

    public Builder setSignPayload(boolean signPayload) {
      this.signPayload = signPayload;
      return this;
    }

    public Builder setUnfilteredURI(String uri) {
      this.unfilteredURI = uri;
      return this;
    }

    public Builder setStringToSign(String stringToSign) {
      this.stringToSign = stringToSign;
      return this;
    }

    public SignatureInfo build() {
      return new SignatureInfo(this);
    }
  }
}
