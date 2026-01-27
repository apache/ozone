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

/**
 * JAXB model for AWS STS AssumeRoleResponse.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "AssumeRoleResponse", namespace = "https://sts.amazonaws.com/doc/2011-06-15/")
public class S3AssumeRoleResponseXml {

  @XmlElement(name = "AssumeRoleResult")
  private AssumeRoleResult assumeRoleResult;

  @XmlElement(name = "ResponseMetadata")
  private ResponseMetadata responseMetadata;

  public AssumeRoleResult getAssumeRoleResult() {
    return assumeRoleResult;
  }

  public void setAssumeRoleResult(AssumeRoleResult assumeRoleResult) {
    this.assumeRoleResult = assumeRoleResult;
  }

  public ResponseMetadata getResponseMetadata() {
    return responseMetadata;
  }

  public void setResponseMetadata(ResponseMetadata responseMetadata) {
    this.responseMetadata = responseMetadata;
  }

  /**
   * AssumeRoleResult element.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class AssumeRoleResult {
    @XmlElement(name = "Credentials")
    private Credentials credentials;

    @XmlElement(name = "AssumedRoleUser")
    private AssumedRoleUser assumedRoleUser;

    public Credentials getCredentials() {
      return credentials;
    }

    public void setCredentials(Credentials credentials) {
      this.credentials = credentials;
    }

    public AssumedRoleUser getAssumedRoleUser() {
      return assumedRoleUser;
    }

    public void setAssumedRoleUser(AssumedRoleUser assumedRoleUser) {
      this.assumedRoleUser = assumedRoleUser;
    }
  }

  /**
   * Credentials element.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
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

  /**
   * AssumedRoleId element.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
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

  /**
   * ResponseMetadata element.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
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


