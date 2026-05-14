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

package org.apache.hadoop.ozone.s3sts;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * JAXB model for AWS-compatible AssumeRoleWithWebIdentity response XML.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "AssumeRoleWithWebIdentityResponse",
    namespace = "https://sts.amazonaws.com/doc/2011-06-15/")
public class S3AssumeRoleWithWebIdentityResponseXml {

  @XmlElement(name = "AssumeRoleWithWebIdentityResult")
  private AssumeRoleWithWebIdentityResult result;

  @XmlElement(name = "ResponseMetadata")
  private ResponseMetadata responseMetadata;

  public AssumeRoleWithWebIdentityResult getResult() {
    return result;
  }

  public void setResult(AssumeRoleWithWebIdentityResult result) {
    this.result = result;
  }

  public ResponseMetadata getResponseMetadata() {
    return responseMetadata;
  }

  public void setResponseMetadata(ResponseMetadata responseMetadata) {
    this.responseMetadata = responseMetadata;
  }

  /** XML model for the AssumeRoleWithWebIdentityResult element. */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "AssumeRoleWithWebIdentityResultType",
      namespace = "https://sts.amazonaws.com/doc/2011-06-15/")
  public static class AssumeRoleWithWebIdentityResult {
    @XmlElement(name = "Credentials")
    private Credentials credentials;

    @XmlElement(name = "SubjectFromWebIdentityToken")
    private String subjectFromWebIdentityToken;

    @XmlElement(name = "AssumedRoleUser")
    private AssumedRoleUser assumedRoleUser;

    @XmlElement(name = "Audience")
    private String audience;

    @XmlElement(name = "Provider")
    private String provider;

    public Credentials getCredentials() {
      return credentials;
    }

    public void setCredentials(Credentials credentials) {
      this.credentials = credentials;
    }

    public String getSubjectFromWebIdentityToken() {
      return subjectFromWebIdentityToken;
    }

    public void setSubjectFromWebIdentityToken(String value) {
      this.subjectFromWebIdentityToken = value;
    }

    public AssumedRoleUser getAssumedRoleUser() {
      return assumedRoleUser;
    }

    public void setAssumedRoleUser(AssumedRoleUser assumedRoleUser) {
      this.assumedRoleUser = assumedRoleUser;
    }

    public String getAudience() {
      return audience;
    }

    public void setAudience(String audience) {
      this.audience = audience;
    }

    public String getProvider() {
      return provider;
    }

    public void setProvider(String provider) {
      this.provider = provider;
    }
  }

  /** XML model for temporary STS credentials. */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "AssumeRoleWithWebIdentityCredentialsType",
      namespace = "https://sts.amazonaws.com/doc/2011-06-15/")
  public static class Credentials {
    @XmlElement(name = "AccessKeyId")
    private String accessKeyId;

    @XmlElement(name = "SecretAccessKey")
    private String secretAccessKey;

    @XmlElement(name = "SessionToken")
    private String sessionToken;

    @XmlElement(name = "Expiration")
    private String expiration;

    public String getAccessKeyId() {
      return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
      this.accessKeyId = accessKeyId;
    }

    public String getSecretAccessKey() {
      return secretAccessKey;
    }

    public void setSecretAccessKey(String secretAccessKey) {
      this.secretAccessKey = secretAccessKey;
    }

    public String getSessionToken() {
      return sessionToken;
    }

    public void setSessionToken(String sessionToken) {
      this.sessionToken = sessionToken;
    }

    public String getExpiration() {
      return expiration;
    }

    public void setExpiration(String expiration) {
      this.expiration = expiration;
    }
  }

  /** XML model for the assumed role user. */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "AssumeRoleWithWebIdentityAssumedRoleUserType",
      namespace = "https://sts.amazonaws.com/doc/2011-06-15/")
  public static class AssumedRoleUser {
    @XmlElement(name = "AssumedRoleId")
    private String assumedRoleId;

    @XmlElement(name = "Arn")
    private String arn;

    public String getAssumedRoleId() {
      return assumedRoleId;
    }

    public void setAssumedRoleId(String assumedRoleId) {
      this.assumedRoleId = assumedRoleId;
    }

    public String getArn() {
      return arn;
    }

    public void setArn(String arn) {
      this.arn = arn;
    }
  }

  /** XML model for response metadata. */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "AssumeRoleWithWebIdentityResponseMetadataType",
      namespace = "https://sts.amazonaws.com/doc/2011-06-15/")
  public static class ResponseMetadata {
    @XmlElement(name = "RequestId")
    private String requestId;

    public String getRequestId() {
      return requestId;
    }

    public void setRequestId(String requestId) {
      this.requestId = requestId;
    }
  }
}
