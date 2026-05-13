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

    public void setCredentials(Credentials credentials) {
      this.credentials = credentials;
    }

    public void setSubjectFromWebIdentityToken(String value) {
      this.subjectFromWebIdentityToken = value;
    }

    public void setAssumedRoleUser(AssumedRoleUser assumedRoleUser) {
      this.assumedRoleUser = assumedRoleUser;
    }

    public void setAudience(String audience) {
      this.audience = audience;
    }

    public void setProvider(String provider) {
      this.provider = provider;
    }
  }

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

    public void setAccessKeyId(String accessKeyId) {
      this.accessKeyId = accessKeyId;
    }

    public void setSecretAccessKey(String secretAccessKey) {
      this.secretAccessKey = secretAccessKey;
    }

    public void setSessionToken(String sessionToken) {
      this.sessionToken = sessionToken;
    }

    public void setExpiration(String expiration) {
      this.expiration = expiration;
    }
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "AssumeRoleWithWebIdentityAssumedRoleUserType",
      namespace = "https://sts.amazonaws.com/doc/2011-06-15/")
  public static class AssumedRoleUser {
    @XmlElement(name = "AssumedRoleId")
    private String assumedRoleId;

    @XmlElement(name = "Arn")
    private String arn;

    public void setAssumedRoleId(String assumedRoleId) {
      this.assumedRoleId = assumedRoleId;
    }

    public void setArn(String arn) {
      this.arn = arn;
    }
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlType(name = "AssumeRoleWithWebIdentityResponseMetadataType",
      namespace = "https://sts.amazonaws.com/doc/2011-06-15/")
  public static class ResponseMetadata {
    @XmlElement(name = "RequestId")
    private String requestId;

    public void setRequestId(String requestId) {
      this.requestId = requestId;
    }
  }
}
