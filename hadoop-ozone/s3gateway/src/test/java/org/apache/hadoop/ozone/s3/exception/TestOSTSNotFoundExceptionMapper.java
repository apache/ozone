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

package org.apache.hadoop.ozone.s3.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.StringReader;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.hadoop.ozone.s3.RequestIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

/**
 * Unit tests for {@link OSTSNotFoundExceptionMapper}.
 */
public class TestOSTSNotFoundExceptionMapper {
  private static final String REQUEST_ID = "test-request-id";
  private static final String STS_NS = "https://sts.amazonaws.com/doc/2011-06-15/";

  private OSTSNotFoundExceptionMapper mapper;

  @BeforeEach
  public void setup() {
    mapper = new OSTSNotFoundExceptionMapper();
    final RequestIdentifier requestIdentifier = mock(RequestIdentifier.class);
    when(requestIdentifier.getRequestId()).thenReturn(REQUEST_ID);
    mapper.setRequestIdentifier(requestIdentifier);
  }

  @Test
  public void testMapsNotFoundToStsValidationErrorForStsPath() throws Exception {
    mapper.setUriInfo(createUriInfo("sts"));

    try (Response response = mapper.toResponse(new NotFoundException())) {

      assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
      assertEquals(MediaType.APPLICATION_XML_TYPE, response.getMediaType());
      assertStsValidationErrorXml((String) response.getEntity(), "/sts");
    }
  }

  @Test
  public void testMapsNotFoundToStsValidationErrorForUnknownPath() throws Exception {
    mapper.setUriInfo(createUriInfo("foo/bar"));

    try (Response response = mapper.toResponse(new NotFoundException())) {

      assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
      assertEquals(MediaType.APPLICATION_XML_TYPE, response.getMediaType());
      assertStsValidationErrorXml((String) response.getEntity(), "/foo/bar");
    }
  }

  @Test
  public void testMapsNotFoundWhenRequestContextIsUnavailable() throws Exception {
    try (Response response = mapper.toResponse(new NotFoundException())) {

      assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
      assertEquals(MediaType.APPLICATION_XML_TYPE, response.getMediaType());
      assertStsValidationErrorXml((String) response.getEntity(), "/");
    }
  }

  private static UriInfo createUriInfo(String path) {
    final UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getPath()).thenReturn(path);
    return uriInfo;
  }

  private static void assertStsValidationErrorXml(String xml, String expectedPathInMessage) throws Exception {
    final Document doc = parseXml(xml);
    final Element root = doc.getDocumentElement();
    assertEquals("ErrorResponse", root.getLocalName());
    assertEquals(STS_NS, root.getNamespaceURI());
    assertEquals("Sender", doc.getElementsByTagName("Type").item(0).getTextContent());
    assertEquals("ValidationError", doc.getElementsByTagName("Code").item(0).getTextContent());
    assertEquals(REQUEST_ID, doc.getElementsByTagName("RequestId").item(0).getTextContent());

    final String message = doc.getElementsByTagName("Message").item(0).getTextContent();
    assertTrue(
        message.contains("Invalid STS endpoint path '" + expectedPathInMessage + "'"),
        "Expected message to mention path: " + expectedPathInMessage);
    assertTrue(message.contains("root path /"), "Expected message to mention root path");
  }

  private static Document parseXml(String xml) throws Exception {
    assertNotNull(xml);
    final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    documentBuilderFactory.setNamespaceAware(true);
    final DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
    return documentBuilder.parse(new InputSource(new StringReader(xml)));
  }
}
