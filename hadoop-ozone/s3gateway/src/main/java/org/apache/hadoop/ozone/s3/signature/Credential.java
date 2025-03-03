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

/**
 * Credential in the AWS authorization header.
 * Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/
 * sigv4-auth-using-authorization-header.html
 */
public class Credential {

  private String accessKeyID;
  private String date;
  private String awsRegion;
  private String awsService;
  private String awsRequest;
  private String credential;

  /**
   * Construct Credential Object.
   *
   * @param cred
   */
  Credential(String cred) throws MalformedResourceException {
    this.credential = cred;
    parseCredential();
  }

  /**
   * Parse credential value.
   * <p>
   * Sample credential value:
   * Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request
   *
   * @throws MalformedResourceException
   */
  @SuppressWarnings("StringSplitter")
  public void parseCredential() throws MalformedResourceException {
    String[] split = credential.split("/");
    switch (split.length) {
    case 5:
      // Ex: dkjad922329ddnks/20190321/us-west-1/s3/aws4_request
      accessKeyID = split[0].trim();
      date = split[1].trim();
      awsRegion = split[2].trim();
      awsService = split[3].trim();
      awsRequest = split[4].trim();
      return;
    case 6:
      // Access id is kerberos principal.
      // Ex: testuser/om@EXAMPLE.COM/20190321/us-west-1/s3/aws4_request
      accessKeyID = split[0] + "/" + split[1];
      date = split[2].trim();
      awsRegion = split[3].trim();
      awsService = split[4].trim();
      awsRequest = split[5].trim();
      return;
    default:
      throw new MalformedResourceException(
          "Credentials not in expected format.", credential);
    }
  }

  public String getAccessKeyID() {
    return accessKeyID;
  }

  public String getDate() {
    return date;
  }

  public String getAwsRegion() {
    return awsRegion;
  }

  public String getAwsService() {
    return awsService;
  }

  public String getAwsRequest() {
    return awsRequest;
  }

  public String getCredential() {
    return credential;
  }

  public String createScope() {
    return String.format("%s/%s/%s/%s", getDate(),
        getAwsRegion(), getAwsService(),
        getAwsRequest());
  }
}
