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
 * JAXB model for AWS STS GetCallerIdentityResponse.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "GetCallerIdentityResponse", namespace = "https://sts.amazonaws.com/doc/2011-06-15/")
public class S3GetCallerIdentityResponseXml {

  @XmlElement(name = "GetCallerIdentityResult")
  private GetCallerIdentityResult getCallerIdentityResult;

  @XmlElement(name = "ResponseMetadata")
  private S3STSResponseMetadata responseMetadata;

  public GetCallerIdentityResult getGetCallerIdentityResult() {
    return getCallerIdentityResult;
  }

  public void setGetCallerIdentityResult(GetCallerIdentityResult getCallerIdentityResult) {
    this.getCallerIdentityResult = getCallerIdentityResult;
  }

  public S3STSResponseMetadata getResponseMetadata() {
    return responseMetadata;
  }

  public void setResponseMetadata(S3STSResponseMetadata responseMetadata) {
    this.responseMetadata = responseMetadata;
  }

  /**
   * GetCallerIdentityResult element.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class GetCallerIdentityResult {
    @XmlElement(name = "Arn")
    private String arn;

    @XmlElement(name = "UserId")
    private String userId;

    @XmlElement(name = "Account")
    private String account;

    public String getArn() {
      return arn;
    }

    public void setArn(String arn) {
      this.arn = arn;
    }

    public String getUserId() {
      return userId;
    }

    public void setUserId(String userId) {
      this.userId = userId;
    }

    public String getAccount() {
      return account;
    }

    public void setAccount(String account) {
      this.account = account;
    }
  }
}
