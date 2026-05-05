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

package org.apache.hadoop.ozone.s3.commontypes;

import static org.apache.hadoop.ozone.s3.util.S3Consts.ENCODING_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import org.junit.jupiter.api.Test;

/**
 * Testing on object key name serialization.
 */
public class TestObjectKeyNameAdapter {
  @Test
  public void testEncodeResult() throws Exception {
    assertEquals("abc/", getAdapter()
        .marshal(EncodingTypeObject.createNullable("abc/", ENCODING_TYPE)));
    assertEquals("a+b+c/", getAdapter()
        .marshal(EncodingTypeObject.createNullable("a b c/", ENCODING_TYPE)));
    assertEquals("a%2Bb%2Bc/", getAdapter()
        .marshal(EncodingTypeObject.createNullable("a+b+c/", ENCODING_TYPE)));

    assertEquals("abc/", getAdapter()
        .marshal(EncodingTypeObject.createNullable("abc/", null)));
    assertEquals("a b c/", getAdapter()
        .marshal(EncodingTypeObject.createNullable("a b c/", null)));
    assertEquals("a+b+c/", getAdapter()
        .marshal(EncodingTypeObject.createNullable("a+b+c/", null)));
  }

  private XmlAdapter<String, EncodingTypeObject> getAdapter() {
    return (new ObjectKeyNameAdapter());
  }
}
