/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.security;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link AWSV4AuthValidator}.
 * */
@RunWith(Parameterized.class)
public class TestAWSV4AuthValidator {

  private String strToSign;
  private String signature;
  private String awsAccessKey;
  private Boolean result;

  public TestAWSV4AuthValidator(String strToSign, String signature,
      String awsAccessKey, Boolean result) {
    this.strToSign = strToSign;
    this.signature = signature;
    this.awsAccessKey = awsAccessKey;
    this.result = result;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {
            "AWS4-HMAC-SHA256\n" +
                "20190221T002037Z\n" +
                "20190221/us-west-1/s3/aws4_request\n" +
                "c297c080cce4e0927779823d3fd1f5cae71481a8f7dfc7e18d" +
                "91851294efc47d",
            "56ec73ba1974f8feda8365c3caef89c5d4a688d5f9baccf" +
                "4765f46a14cd745ad",
            "dbaksbzljandlkandlsd",
            true
        },
        {
            "AWS4-HMAC-SHA256\n" +
                "20150830T123600Z\n" +
                "20150830/us-east-1/iam/aws4_request\n" +
                "f536975d06c0309214f805bb90ccff089219ecd68b2" +
                "577efef23edd43b7e1a59",
            "5d672d79c15b13162d9279b0855cfba" +
                "6789a8edb4c82c400e06b5924a6f2b5d7",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            true
        },
        // Invalid Algorithm
        {
            "AWS4-ZAVC-HJUA123\n" +
                "20150830T123600Z\n" +
                "20150830/us-east-1/iam/aws4_request\n" +
                "f536975d06c0309214f805bb90ccff089219ecd68b2" +
                "577efef23edd43b7e1a59",
            "5d672d79c15b13162d9279b0855cfba" +
                "6789a8edb4c82c400e06b5924a6f2b5d7",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            false
        },
        // Invalid timestamp or non-ISO timestamp format
        {
            "AWS4-HMAC-SHA256\n" +
                "Thu, 21 Feb 2019 00:20:37 -0800\n" +
                "20190221/us-west-1/s3/aws4_request\n" +
                "c297c080cce4e0927779823d3fd1f5cae71481a8f7dfc7e18d" +
                "91851294efc47d",
            "56ec73ba1974f8feda8365c3caef89c5d4a688d5f9baccf" +
                "4765f46a14cd745ad",
            "dbaksbzljandlkandlsd",
            false
        },
        // Invalid scope. Uppercase letters in AWS service.
        {
            "AWS4-HMAC-SHA256\n" +
                "20190221T002037Z\n" +
                "20190221/us-west-1/S3/aws4_request\n" +
                "c297c080cce4e0927779823d3fd1f5cae71481a8f7dfc7e18d" +
                "91851294efc47d",
            "56ec73ba1974f8feda8365c3caef89c5d4a688d5f9baccf" +
                "4765f46a14cd745ad",
            "dbaksbzljandlkandlsd",
            false
        },
        // Invalid hex of canonical request. Less than 64 hex characters.
        {
            "AWS4-HMAC-SHA256\n" +
                "20190221T002037Z\n" +
                "20190221/us-west-1/s3/aws4_request\n" +
                "c297c080cce4e0927779823d3fd1f5cae71481a8f7dfc7e18d" +
                "91851294efc47d",
            "56ec73ba1974f8feda8365c3caef89c5d4a688d5f9baccf" +
                "4765f46a14cd745a",
            "dbaksbzljandlkandlsd",
            false
        },
        // AWS V2 request
        {
            "PUT\n" +
                "\n" +
                "application/octet-stream\n" +
                "Thu, 09 Mar 2023 18:57:26 -0800\n" +
                "/bucket1/1.txt",
            "CevEya+7yP3B0zqYoyTz2aVDuP0=",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            false
        }
    });
  }

  @Test
  public void testValidateRequest() {
    assertEquals(result, AWSV4AuthValidator.validateRequest(
            strToSign, signature, awsAccessKey));
  }
}
