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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.Test;

/**
 * Class tests Unmarshall logic of {@link CompleteMultipartUploadRequest}.
 */
public class TestCompleteMultipartUploadRequestUnmarshaller {

  private static String part1 = UUID.randomUUID().toString();
  private static String part2 = UUID.randomUUID().toString();

  @Test
  public void fromStreamWithNamespace() throws IOException {
    //GIVEN
    ByteArrayInputStream inputBody =
        new ByteArrayInputStream(
            ("<CompleteMultipartUpload xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
                "<Part><ETag>" + part1 + "</ETag><PartNumber>1" +
                "</PartNumber></Part><Part><ETag>" + part2 +
                "</ETag><PartNumber>2</PartNumber></Part>" +
                "</CompleteMultipartUpload>")
                .getBytes(UTF_8));

    //WHEN
    CompleteMultipartUploadRequest completeMultipartUploadRequest =
        unmarshall(inputBody);

    //THEN
    checkContent(completeMultipartUploadRequest);
  }

  @Test
  public void fromStreamWithoutNamespace() throws IOException {
    //GIVEN
    ByteArrayInputStream inputBody =
        new ByteArrayInputStream(
            ("<CompleteMultipartUpload>" +
                "<Part><ETag>" + part1 + "</ETag><PartNumber>1</PartNumber" +
                "></Part><Part><ETag>" + part2 + "</ETag><PartNumber>2" +
                "</PartNumber></Part></CompleteMultipartUpload>")
                .getBytes(UTF_8));

    //WHEN
    CompleteMultipartUploadRequest completeMultipartUploadRequest =
        unmarshall(inputBody);

    //THEN
    checkContent(completeMultipartUploadRequest);
  }

  private void checkContent(CompleteMultipartUploadRequest request) {
    assertNotNull(request);
    assertEquals(2, request.getPartList().size());

    List<CompleteMultipartUploadRequest.Part> parts =
        request.getPartList();

    assertEquals(part1, parts.get(0).getETag());
    assertEquals(part2, parts.get(1).getETag());
  }

  private CompleteMultipartUploadRequest unmarshall(
      ByteArrayInputStream inputBody) throws IOException {
    return new CompleteMultipartUploadRequestUnmarshaller()
        .readFrom(null, null, null, null, null, inputBody);
  }

  @Test
  public void concurrentParse() {
    CompleteMultipartUploadRequestUnmarshaller unmarshaller =
        new CompleteMultipartUploadRequestUnmarshaller();
    byte[] bytes = ("<CompleteMultipartUpload>" + "<Part><ETag>" + part1 +
        "</ETag><PartNumber>1</PartNumber" + "></Part><Part><ETag>" +
        part2 + "</ETag><PartNumber>2" +
        "</PartNumber></Part></CompleteMultipartUpload>").getBytes(
        UTF_8);

    List<CompletableFuture<CompleteMultipartUploadRequest>> futures =
        new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      futures.add(CompletableFuture.supplyAsync(() -> {
        try {
          //GIVEN
          ByteArrayInputStream inputBody = new ByteArrayInputStream(bytes);
          //WHEN
          return unmarshall(unmarshaller, inputBody);
        } catch (IOException e) {
          return null;
        }
      }));
    }

    for (CompletableFuture<CompleteMultipartUploadRequest> future : futures) {
      CompleteMultipartUploadRequest request = future.join();
      //THEN
      checkContent(request);
    }
  }

  private CompleteMultipartUploadRequest unmarshall(
      CompleteMultipartUploadRequestUnmarshaller unmarshaller,
      ByteArrayInputStream inputBody) throws IOException {
    return unmarshaller
        .readFrom(null, null, null, null, null, inputBody);
  }
}
