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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import jakarta.servlet.http.HttpServletResponse;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link S3ContentTypeFilter}.
 */
public class TestS3ContentTypeFilter {

  private HttpServletResponse wrap(HttpServletResponse delegate) throws Exception {
    AtomicReference<HttpServletResponse> wrapped = new AtomicReference<>();
    new S3ContentTypeFilter().doFilter(mock(jakarta.servlet.http.HttpServletRequest.class),
        delegate, (request, response) -> wrapped.set((HttpServletResponse) response));
    return wrapped.get();
  }

  @Test
  public void stripsCharsetFromXmlSetHeader() throws Exception {
    HttpServletResponse delegate = mock(HttpServletResponse.class);
    wrap(delegate).setHeader("Content-Type", "application/xml;charset=utf-8");
    verify(delegate).setHeader("Content-Type", "application/xml");
  }

  @Test
  public void stripsCharsetFromXmlSetContentType() throws Exception {
    HttpServletResponse delegate = mock(HttpServletResponse.class);
    wrap(delegate).setContentType("application/xml;charset=UTF-8");
    verify(delegate).setContentType("application/xml");
  }

  @Test
  public void stripsCharsetFromXmlAddHeader() throws Exception {
    HttpServletResponse delegate = mock(HttpServletResponse.class);
    wrap(delegate).addHeader("Content-Type", "application/xml; charset=utf-8");
    verify(delegate).addHeader("Content-Type", "application/xml");
  }

  @Test
  public void leavesNonXmlContentTypeUnchanged() throws Exception {
    HttpServletResponse delegate = mock(HttpServletResponse.class);
    wrap(delegate).setHeader("Content-Type", "application/json;charset=utf-8");
    verify(delegate).setHeader("Content-Type", "application/json;charset=utf-8");
  }

  @Test
  public void leavesOtherHeadersUnchanged() throws Exception {
    HttpServletResponse delegate = mock(HttpServletResponse.class);
    wrap(delegate).setHeader("ETag", "\"abc\"");
    verify(delegate).setHeader("ETag", "\"abc\"");
  }
}
