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

package org.apache.hadoop.ozone.s3;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static org.apache.hadoop.ozone.s3.util.S3Consts.READ_CONSISTENCY_HEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests {@link ReadConsistencyFilter}.
 */
public class TestReadConsistencyFilter {

  @ParameterizedTest
  @ValueSource(strings = {"invalid", "follower-stale;logLimit=10"})
  void rejectsInvalidReadConsistencyHeader(String value) {
    ContainerRequestContext context = mock(ContainerRequestContext.class);
    when(context.getHeaderString(READ_CONSISTENCY_HEADER)).thenReturn(value);

    WebApplicationException ex = assertThrows(WebApplicationException.class,
        () -> new ReadConsistencyFilter().filter(context));

    assertEquals(HTTP_BAD_REQUEST, ex.getResponse().getStatus());
    assertTrue(ex.getResponse().getEntity().toString()
        .contains("InvalidArgument"));
  }
}
