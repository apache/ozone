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

package org.apache.hadoop.ozone.s3.signature;

import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * This class tests Authorization header format v2.
 */
public class TestAuthorizationV2HeaderParser {

  @Test
  public void testAuthHeaderV2() throws OS3Exception {
    try {
      String auth = "AWS accessKey:signature";
      AuthorizationV2HeaderParser v2 = new AuthorizationV2HeaderParser(auth);
      final SignatureInfo signatureInfo = v2.parseSignature();
      assertEquals(signatureInfo.getAwsAccessId(), "accessKey");
      assertEquals(signatureInfo.getSignature(), "signature");
    } catch (OS3Exception ex) {
      fail("testAuthHeaderV2 failed");
    }
  }

  @Test
  public void testIncorrectHeader1() throws OS3Exception {
    String auth = "AAA accessKey:signature";
    AuthorizationV2HeaderParser v2 = new AuthorizationV2HeaderParser(auth);
    Assert.assertNull(v2.parseSignature());

  }

  @Test
  public void testIncorrectHeader2() throws OS3Exception {
    try {
      String auth = "AWS :accessKey";
      AuthorizationV2HeaderParser v2 = new AuthorizationV2HeaderParser(auth);
      Assert.assertNull(v2.parseSignature());
      fail("testIncorrectHeader");
    } catch (OS3Exception ex) {
      assertEquals("AuthorizationHeaderMalformed", ex.getCode());
    }
  }

  @Test
  public void testIncorrectHeader3() throws OS3Exception {
    try {
      String auth = "AWS :signature";
      AuthorizationV2HeaderParser v2 = new AuthorizationV2HeaderParser(auth);
      Assert.assertNull(v2.parseSignature());
      fail("testIncorrectHeader");
    } catch (OS3Exception ex) {
      assertEquals("AuthorizationHeaderMalformed", ex.getCode());
    }
  }

  @Test
  public void testIncorrectHeader4() throws OS3Exception {
    try {
      String auth = "AWS accessKey:";
      AuthorizationV2HeaderParser v2 = new AuthorizationV2HeaderParser(auth);
      Assert.assertNull(v2.parseSignature());
      fail("testIncorrectHeader");
    } catch (OS3Exception ex) {
      assertEquals("AuthorizationHeaderMalformed", ex.getCode());
    }
  }
}
