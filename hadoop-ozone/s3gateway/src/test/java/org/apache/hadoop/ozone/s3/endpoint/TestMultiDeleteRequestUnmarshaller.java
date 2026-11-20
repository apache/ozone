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

package org.apache.hadoop.ozone.s3.endpoint;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.ws.rs.WebApplicationException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.Test;

/**
 * Test custom marshalling of MultiDeleteRequest.
 */
public class TestMultiDeleteRequestUnmarshaller {

  @Test
  public void fromStreamWithNamespace() throws IOException {
    //GIVEN
    ByteArrayInputStream inputBody =
        new ByteArrayInputStream(
            ("<Delete xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\"><Object>key1</Object><Object>key2"
                + "</Object><Object>key3"
                + "</Object></Delete>")
                .getBytes(UTF_8));

    //WHEN
    MultiDeleteRequest multiDeleteRequest =
        unmarshall(inputBody);

    //THEN
    assertEquals(3, multiDeleteRequest.getObjects().size());
  }

  @Test
  public void fromStreamWithoutNamespace() throws IOException {
    //GIVEN
    ByteArrayInputStream inputBody =
        new ByteArrayInputStream(
            ("<Delete><Object>key1</Object><Object>key2"
                + "</Object><Object>key3"
                + "</Object></Delete>")
                .getBytes(UTF_8));

    //WHEN
    MultiDeleteRequest multiDeleteRequest =
        unmarshall(inputBody);

    //THEN
    assertEquals(3, multiDeleteRequest.getObjects().size());
  }

  @Test
  public void recordsMd5OfRawBody() throws IOException {
    byte[] body = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<Delete><Object><Key>delete-me.txt</Key></Object></Delete>\n").getBytes(UTF_8);

    MultiDeleteRequest request = unmarshall(new ByteArrayInputStream(body));

    assertEquals("delete-me.txt", request.getObjects().get(0).getKey());
    assertEquals(DigestUtils.md5Hex(body), request.getBodyContentMD5());
  }

  @Test
  public void bodyReadFailureIsInvalidRequest() {
    InputStream failing = new InputStream() {
      @Override
      public int read() throws IOException {
        throw new IOException("connection reset");
      }
    };

    WebApplicationException ex = assertThrows(WebApplicationException.class,
        () -> new MultiDeleteRequestUnmarshaller().readFrom(null, null, null, null, null, failing));

    assertEquals(S3ErrorTable.INVALID_REQUEST.getHttpCode(), ex.getResponse().getStatus());
    assertEquals(S3ErrorTable.INVALID_REQUEST.getCode(), ((OS3Exception) ex.getCause()).getCode());
  }

  @Test
  public void bodyOverSizeLimitIsRejected() {
    InputStream endless = new InputStream() {
      @Override
      public int read() {
        return ' ';
      }
    };

    WebApplicationException ex = assertThrows(WebApplicationException.class,
        () -> new MultiDeleteRequestUnmarshaller().readFrom(null, null, null, null, null, endless));

    assertEquals(S3ErrorTable.PAYLOAD_TOO_LARGE.getHttpCode(), ex.getResponse().getStatus());
    assertEquals(S3ErrorTable.PAYLOAD_TOO_LARGE.getCode(), ((OS3Exception) ex.getCause()).getCode());
  }

  private MultiDeleteRequest unmarshall(ByteArrayInputStream inputBody)
      throws IOException {
    return new MultiDeleteRequestUnmarshaller()
        .readFrom(null, null, null, null, null, inputBody);
  }
}
