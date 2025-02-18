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

package org.apache.hadoop.ozone.om.protocol;

/**
 * S3Auth wraps the data needed for S3 Authentication.
 */
public class S3Auth {
  private String stringToSign;
  private String signature;
  private String accessID;
  public static final String S3_AUTH_CHECK = "ozone.s3.auth.check";
  // User principal to be used for KMS encryption and decryption
  private String userPrincipal;

  public S3Auth(final String stringToSign,
                final String signature,
                final String accessID,
                final String userPrincipal) {
    this.stringToSign = stringToSign;
    this.signature = signature;
    this.accessID = accessID;
    this.userPrincipal = userPrincipal;
  }

  public String getStringTosSign() {
    return stringToSign;
  }

  public String getSignature() {
    return signature;
  }

  public String getAccessID() {
    return accessID;
  }

  public String getUserPrincipal() {
    return userPrincipal;
  }

  public void setUserPrincipal(String userPrincipal) {
    this.userPrincipal = userPrincipal;
  }
}
