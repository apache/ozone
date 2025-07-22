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

import java.io.ByteArrayInputStream;
import java.io.IOException;
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

  private MultiDeleteRequest unmarshall(ByteArrayInputStream inputBody)
      throws IOException {
    return new MultiDeleteRequestUnmarshaller()
        .readFrom(null, null, null, null, null, inputBody);
  }
}
