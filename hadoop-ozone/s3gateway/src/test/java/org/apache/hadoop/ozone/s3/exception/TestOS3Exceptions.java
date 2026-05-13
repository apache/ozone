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

package org.apache.hadoop.ozone.s3.exception;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.junit.jupiter.api.Test;

/**
 * This class tests OS3Exception class.
 */
public class TestOS3Exceptions {

  @Test
  public void testOS3Exceptions() {
    OS3Exception ex = S3ErrorTable.newError(S3ErrorTable.ACCESS_DENIED, "bucket");
    ex.setRequestId(OzoneUtils.getRequestID());
    String val = ex.toXml();
    String formatString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>%n" +
        "<Error>%n" +
        "  <Code>%s</Code>%n" +
        "  <Message>%s</Message>%n" +
        "  <Resource>%s</Resource>%n" +
        "  <RequestId>%s</RequestId>%n" +
        "</Error>%n";
    String expected = String.format(formatString, ex.getCode(),
        ex.getErrorMessage(), ex.getResource(),
        ex.getRequestId());
    assertEquals(expected, val);
  }

  /**
   * AWS S3 returns HTTP 400 Bad Request for ExpiredToken (not 403).
   */
  @Test
  public void testExpiredTokenUsesBadRequestHttpStatus() {
    assertEquals(HTTP_BAD_REQUEST, S3ErrorTable.EXPIRED_TOKEN.getHttpCode());
    final OS3Exception fromTable = S3ErrorTable.newError(S3ErrorTable.EXPIRED_TOKEN, "resource");
    assertEquals(HTTP_BAD_REQUEST, fromTable.getHttpCode());
  }

  @Test
  public void testOS3ExceptionWithToken0() {
    final OS3Exception ex = S3ErrorTable.newError(S3ErrorTable.EXPIRED_TOKEN, "resource");
    ex.setRequestId(OzoneUtils.getRequestID());
    ex.setHostId(OzoneUtils.getRequestID());
    ex.setToken0("token-value");

    final String val = ex.toXml();
    final String formatString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>%n" +
        "<Error>%n" +
        "  <Code>%s</Code>%n" +
        "  <Message>%s</Message>%n" +
        "  <Resource>%s</Resource>%n" +
        "  <RequestId>%s</RequestId>%n" +
        "  <HostId>%s</HostId>%n" +
        "  <Token-0>%s</Token-0>%n" +
        "</Error>%n";
    final String expected = String.format(formatString, ex.getCode(), ex.getErrorMessage(), ex.getResource(),
        ex.getRequestId(), ex.getHostId(), ex.getToken0());
    assertEquals(expected, val);
  }
}
