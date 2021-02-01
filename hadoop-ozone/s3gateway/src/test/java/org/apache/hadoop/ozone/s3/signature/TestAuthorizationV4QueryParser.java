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

package org.apache.hadoop.ozone.s3.signature;

import javax.ws.rs.core.MultivaluedMap;

import java.time.ZonedDateTime;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestAuthorizationV4QueryParser {

  @Test(expected = IllegalArgumentException.class)
  public void testExpiredHeaders() throws Exception {

    //GIVEN
    final MultivaluedMap parameters = Mockito.mock(MultivaluedMap.class);
    Mockito.when(parameters.getFirst("X-Amz-Date"))
        .thenReturn("20160801T083241Z");
    Mockito.when(parameters.getFirst("X-Amz-Expires")).thenReturn("10000");
    Mockito.when(parameters.containsKey("X-Amz-Signature")).thenReturn(true);

    AuthorizationV4QueryParser parser =
        new AuthorizationV4QueryParser(parameters);

    //WHEN
    parser.parseSignature();

    //THEN
    Assert.fail("Expired header is not detected");
  }

  @Test()
  public void testUnExpiredHeaders() throws Exception {

    //GIVEN
    final MultivaluedMap parameters = Mockito.mock(MultivaluedMap.class);
    Mockito.when(parameters.getFirst("X-Amz-Date"))
        .thenReturn(
            ZonedDateTime.now().format(StringToSignProducer.TIME_FORMATTER));
    Mockito.when(parameters.getFirst("X-Amz-Expires")).thenReturn("10000");
    Mockito.when(parameters.getFirst("X-Amz-Credential"))
        .thenReturn("AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request");
    Mockito.when(parameters.containsKey("X-Amz-Signature")).thenReturn(true);

    AuthorizationV4QueryParser parser =
        new AuthorizationV4QueryParser(parameters);

    //WHEN
    parser.parseSignature();

    //THEN
    //passed
  }


  @Test()
  public void testWithoutEpiration() throws Exception {

    //GIVEN
    final MultivaluedMap parameters = Mockito.mock(MultivaluedMap.class);
    Mockito.when(parameters.getFirst("X-Amz-Date"))
        .thenReturn(
            ZonedDateTime.now().format(StringToSignProducer.TIME_FORMATTER));
    Mockito.when(parameters.getFirst("X-Amz-Credential"))
        .thenReturn("AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request");
    Mockito.when(parameters.containsKey("X-Amz-Signature")).thenReturn(true);

    AuthorizationV4QueryParser parser =
        new AuthorizationV4QueryParser(parameters);

    //WHEN
    parser.parseSignature();

    //THEN
    //passed
  }
}