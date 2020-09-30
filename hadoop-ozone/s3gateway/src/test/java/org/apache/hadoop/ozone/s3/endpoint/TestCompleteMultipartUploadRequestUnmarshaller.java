/*
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

package org.apache.hadoop.ozone.s3.endpoint;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

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
            ("<CompleteMultipartUpload xmlns=\"http://s3.amazonaws" +
                ".com/doc/2006-03-01/\">" +
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
    Assert.assertEquals(2, request.getPartList().size());

    List<CompleteMultipartUploadRequest.Part> parts =
        request.getPartList();

    Assert.assertEquals(part1, parts.get(0).geteTag());
    Assert.assertEquals(part2, parts.get(1).geteTag());
  }

  private CompleteMultipartUploadRequest unmarshall(
      ByteArrayInputStream inputBody) throws IOException {
    return new CompleteMultipartUploadRequestUnmarshaller()
        .readFrom(null, null, null, null, null, inputBody);
  }
}
