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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.http.HttpServletRequest;
import org.apache.hadoop.ozone.s3.EmptyContentTypeFilter.EnumerationWrapper;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link EmptyContentTypeFilter}.
 */
public class TestEmptyContentTypeFilter {

  @Test
  public void enumerationWithContentType() {
    Vector<String> values = new Vector<>();
    values.add("Content-Type");
    values.add("1");
    values.add("2");
    values.add("Content-Type");

    final EnumerationWrapper enumerationWrapper =
        new EnumerationWrapper(values.elements());

    assertTrue(enumerationWrapper.hasMoreElements());
    assertEquals(HeaderPreprocessor.ORIGINAL_CONTENT_TYPE,
        enumerationWrapper.nextElement());
    assertTrue(enumerationWrapper.hasMoreElements());
    assertEquals("1", enumerationWrapper.nextElement());
    assertTrue(enumerationWrapper.hasMoreElements());
    assertEquals("2", enumerationWrapper.nextElement());
    assertFalse(enumerationWrapper.hasMoreElements());
  }

  @Test
  public void enumerationWithOneContentType() {
    Vector<String> values = new Vector<>();
    values.add("Content-Type");

    final EnumerationWrapper enumerationWrapper =
        new EnumerationWrapper(values.elements());

    assertTrue(enumerationWrapper.hasMoreElements());
    assertEquals(HeaderPreprocessor.ORIGINAL_CONTENT_TYPE,
        enumerationWrapper.nextElement());
    assertFalse(enumerationWrapper.hasMoreElements());
  }

  @Test
  public void preserveEmptyContentType() throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getContentType()).thenReturn("");
    when(request.getHeaderNames()).thenReturn(
        Collections.enumeration(Collections.singletonList(HeaderPreprocessor.CONTENT_TYPE)));

    AtomicReference<HttpServletRequest> wrappedRequest =
        new AtomicReference<>();
    new EmptyContentTypeFilter().doFilter(request, null,
        (filteredRequest, response) -> wrappedRequest.set(
            (HttpServletRequest) filteredRequest));

    assertNull(wrappedRequest.get().getContentType());
    assertNull(wrappedRequest.get().getHeader(HeaderPreprocessor.CONTENT_TYPE));
    assertEquals("", wrappedRequest.get().getHeader(HeaderPreprocessor.ORIGINAL_CONTENT_TYPE));
    assertEquals(Collections.singletonList(""),
        Collections.list(wrappedRequest.get().getHeaders(HeaderPreprocessor.ORIGINAL_CONTENT_TYPE)));
    assertEquals(Collections.singletonList(
        HeaderPreprocessor.ORIGINAL_CONTENT_TYPE), Collections.list(
        wrappedRequest.get().getHeaderNames()));
  }

}
