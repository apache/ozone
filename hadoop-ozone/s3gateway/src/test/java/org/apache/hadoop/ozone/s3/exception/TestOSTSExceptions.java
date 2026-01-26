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

import java.io.StringReader;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

/**
 * This class tests OSTSException class.  It is named TestOSTSExceptions instead of
 * TestOSTSException to avoid findbugs rule about classes ending in *Exception must
 * extend Exception or Throwable.
 */
public class TestOSTSExceptions {

  @Test
  public void testOSTSException() throws Exception {
    final OSTSException ex = new OSTSException("ValidationError", "1 validation error detected", 400);
    final String requestId = OzoneUtils.getRequestID();
    ex.setRequestId(requestId);
    final String val = ex.toXml();

    final Document doc = parseXml(val);
    final Element root = doc.getDocumentElement();
    assertEquals("ErrorResponse", root.getLocalName());
    // Ensure the response uses the default namespace (no prefix like "ns2:")
    assertEquals("ErrorResponse", root.getNodeName());
    assertEquals("https://sts.amazonaws.com/doc/2011-06-15/", root.getNamespaceURI());

    assertEquals("Sender", doc.getElementsByTagName("Type").item(0).getTextContent());
    assertEquals("ValidationError", doc.getElementsByTagName("Code").item(0).getTextContent());
    assertEquals("1 validation error detected", doc.getElementsByTagName("Message").item(0).getTextContent());
    assertEquals(requestId, doc.getElementsByTagName("RequestId").item(0).getTextContent());
  }

  @Test
  public void testOSTSExceptionInvalidAction() throws Exception {
    final OSTSException ex = new OSTSException("InvalidAction", "Could not find operation", 400);
    final String requestId = OzoneUtils.getRequestID();
    ex.setRequestId(requestId);
    final String val = ex.toXml();

    final Document doc = parseXml(val);
    final Element root = doc.getDocumentElement();
    assertEquals("ErrorResponse", root.getLocalName());
    assertEquals("http://webservices.amazon.com/AWSFault/2005-15-09", root.getNamespaceURI());

    assertEquals("Sender", doc.getElementsByTagName("Type").item(0).getTextContent());
    assertEquals("InvalidAction", doc.getElementsByTagName("Code").item(0).getTextContent());
    assertEquals("Could not find operation", doc.getElementsByTagName("Message").item(0).getTextContent());
    assertEquals(requestId, doc.getElementsByTagName("RequestId").item(0).getTextContent());
  }

  @Test
  public void testOSTSExceptionWithCustomType() throws Exception {
    final OSTSException ex = new OSTSException("InternalFailure", "An internal error has occurred.", 500, "Receiver");
    final String requestId = OzoneUtils.getRequestID();
    ex.setRequestId(requestId);
    final String val = ex.toXml();

    final Document doc = parseXml(val);
    final Element root = doc.getDocumentElement();
    assertEquals("ErrorResponse", root.getLocalName());
    assertEquals("https://sts.amazonaws.com/doc/2011-06-15/", root.getNamespaceURI());

    assertEquals("Receiver", doc.getElementsByTagName("Type").item(0).getTextContent());
    assertEquals("InternalFailure", doc.getElementsByTagName("Code").item(0).getTextContent());
    assertEquals("An internal error has occurred.", doc.getElementsByTagName("Message").item(0).getTextContent());
    assertEquals(requestId, doc.getElementsByTagName("RequestId").item(0).getTextContent());
  }

  private static Document parseXml(String xml) throws Exception {
    assertNotNull(xml);
    final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    documentBuilderFactory.setNamespaceAware(true);
    final DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
    return documentBuilder.parse(new InputSource(new StringReader(xml)));
  }
}
