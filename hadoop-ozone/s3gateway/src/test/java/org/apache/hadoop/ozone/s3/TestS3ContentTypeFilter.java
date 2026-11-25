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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link S3ContentTypeFilter}.
 *
 * <p>The filter preserves the Content-Type verbatim and resets the tracked
 * response encoding for bare values, so Jetty 12 does not append a charset to a
 * value that carries none. A value that already has a charset is left untouched
 * (both the value and the encoding). "Carries a charset" means an actual charset
 * parameter, not the string {@code charset=} appearing anywhere in the value.
 */
public class TestS3ContentTypeFilter {

  private HttpServletResponse wrap(HttpServletResponse delegate) throws Exception {
    AtomicReference<HttpServletResponse> wrapped = new AtomicReference<>();
    new S3ContentTypeFilter().doFilter(mock(HttpServletRequest.class),
        delegate, (request, response) -> wrapped.set((HttpServletResponse) response));
    return wrapped.get();
  }

  @Test
  public void resetsEncodingForBareXmlSetContentType() throws Exception {
    HttpServletResponse delegate = mock(HttpServletResponse.class);
    wrap(delegate).setContentType("application/xml");
    verify(delegate).setCharacterEncoding(null);
    verify(delegate).setContentType("application/xml");
  }

  @Test
  public void resetsEncodingForBareXmlSetHeader() throws Exception {
    HttpServletResponse delegate = mock(HttpServletResponse.class);
    wrap(delegate).setHeader("Content-Type", "application/xml");
    verify(delegate).setCharacterEncoding(null);
    verify(delegate).setHeader("Content-Type", "application/xml");
  }

  @Test
  public void resetsEncodingForBareXmlAddHeader() throws Exception {
    HttpServletResponse delegate = mock(HttpServletResponse.class);
    wrap(delegate).addHeader("Content-Type", "application/xml");
    verify(delegate).setCharacterEncoding(null);
    verify(delegate).addHeader("Content-Type", "application/xml");
  }

  @Test
  public void resetsEncodingForBareNonXmlContentType() throws Exception {
    HttpServletResponse delegate = mock(HttpServletResponse.class);
    wrap(delegate).setHeader("Content-Type", "binary/octet-stream");
    verify(delegate).setCharacterEncoding(null);
    verify(delegate).setHeader("Content-Type", "binary/octet-stream");
  }

  @Test
  public void preservesExplicitCharsetVerbatim() throws Exception {
    HttpServletResponse delegate = mock(HttpServletResponse.class);
    wrap(delegate).setHeader("Content-Type", "application/xml; charset=ISO-8859-1");
    verify(delegate, never()).setCharacterEncoding(null);
    verify(delegate).setHeader("Content-Type", "application/xml; charset=ISO-8859-1");
  }

  @Test
  public void resetsEncodingWhenCharsetOnlyAppearsInAQuotedParameterValue() throws Exception {
    HttpServletResponse delegate = mock(HttpServletResponse.class);
    String contentType = "application/octet-stream; name=\"charset=example\"";
    wrap(delegate).setHeader("Content-Type", contentType);
    verify(delegate).setCharacterEncoding(null);
    verify(delegate).setHeader("Content-Type", contentType);
  }

  @Test
  public void resetsEncodingForParameterNameMerelyEndingInCharset() throws Exception {
    HttpServletResponse delegate = mock(HttpServletResponse.class);
    wrap(delegate).setHeader("Content-Type", "text/plain; x-charset=foo");
    verify(delegate).setCharacterEncoding(null);
    verify(delegate).setHeader("Content-Type", "text/plain; x-charset=foo");
  }

  @Test
  public void preservesRealCharsetFollowingAQuotedParameterValue() throws Exception {
    HttpServletResponse delegate = mock(HttpServletResponse.class);
    String contentType = "application/octet-stream; name=\"a;charset=b\"; charset=iso-8859-1";
    wrap(delegate).setHeader("Content-Type", contentType);
    verify(delegate, never()).setCharacterEncoding(null);
    verify(delegate).setHeader("Content-Type", contentType);
  }

  @Test
  public void preservesDistinctXmlMediaTypeVerbatim() throws Exception {
    HttpServletResponse delegate = mock(HttpServletResponse.class);
    wrap(delegate).setContentType("application/xml-dtd");
    verify(delegate).setContentType("application/xml-dtd");
  }

  @Test
  public void leavesOtherHeadersUnchanged() throws Exception {
    HttpServletResponse delegate = mock(HttpServletResponse.class);
    wrap(delegate).setHeader("ETag", "\"abc\"");
    verify(delegate).setHeader("ETag", "\"abc\"");
    verify(delegate, never()).setCharacterEncoding(null);
  }
}
