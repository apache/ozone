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
package org.apache.hadoop.ozone.s3.header;

import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;



/**
 * Credential in the AWS authorization header.
 * Ref: https://docs.aws.amazon.com/AmazonS3/latest/API/
 * sigv4-auth-using-authorization-header.html
 *
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
   * @param cred
   */
  Credential(String cred) throws OS3Exception {
    this.credential = cred;
    parseCredential();
  }

  /**
   * Parse credential value.
   *
   * Sample credential value:
   * Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request
   *
   * @throws OS3Exception
   */
  public void parseCredential() throws OS3Exception {
    String[] split = credential.split("/");
    if (split.length == 5) {
      accessKeyID = split[0];
      date = split[1];
      awsRegion = split[2];
      awsService = split[3];
      awsRequest = split[4];
    } else {
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_HEADER, credential);
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
}
