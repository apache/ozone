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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * This class tests Authorization header format v2.
 */
public class TestAuthorizationV2HeaderParser {

  @Test
  public void testAuthHeaderV2() throws MalformedResourceException {
    String auth = "AWS accessKey:signature";
    AuthorizationV2HeaderParser v2 = new AuthorizationV2HeaderParser(auth);
    final SignatureInfo signatureInfo = v2.parseSignature();
    assertEquals(signatureInfo.getAwsAccessId(), "accessKey");
    assertEquals(signatureInfo.getSignature(), "signature");
  }

  @Test
  public void testIncorrectHeader1() throws MalformedResourceException {
    String auth = "AAA accessKey:signature";
    AuthorizationV2HeaderParser v2 = new AuthorizationV2HeaderParser(auth);
    assertNull(v2.parseSignature());

  }

  @Test
  public void testIncorrectHeader2() throws MalformedResourceException {
    String auth = "AWS :accessKey";
    AuthorizationV2HeaderParser v2 = new AuthorizationV2HeaderParser(auth);
    assertThrows(MalformedResourceException.class, () -> v2.parseSignature(),
        "testIncorrectHeader");
  }

  @Test
  public void testIncorrectHeader3() throws MalformedResourceException {
    String auth = "AWS :signature";
    AuthorizationV2HeaderParser v2 = new AuthorizationV2HeaderParser(auth);
    assertThrows(MalformedResourceException.class, () -> v2.parseSignature(),
        "testIncorrectHeader");
  }

  @Test
  public void testIncorrectHeader4() throws MalformedResourceException {
    String auth = "AWS accessKey:";
    AuthorizationV2HeaderParser v2 = new AuthorizationV2HeaderParser(auth);
    assertThrows(MalformedResourceException.class, () -> v2.parseSignature(),
        "testIncorrectHeader");
  }
}
