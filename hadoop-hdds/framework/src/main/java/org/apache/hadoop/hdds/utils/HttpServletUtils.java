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

package org.apache.hadoop.hdds.utils;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class HttpServletUtils implements Serializable {
  /**
   * Get the response format from request header.
   *
   * @param request the HTTP servlet request
   * @return {@link ResponseFormat#JSON} if Accept header contains "application/json",
   * otherwise {@link ResponseFormat#XML} (default for backwards compatibility)
   * @see HttpHeaders#ACCEPT
   */
  @VisibleForTesting
  public static ResponseFormat getResponseFormat(HttpServletRequest request) throws IllegalArgumentException {
    String format = request.getHeader(HttpHeaders.ACCEPT);
    if (format == null) {
      return ResponseFormat.UNSPECIFIED;
    }
    return format.contains(ResponseFormat.JSON.getValue()) ?
        ResponseFormat.JSON : ResponseFormat.XML;
  }

  /**
   * Write error response according to the specified format.
   *
   * @param errorMessage the error message
   * @param format       the response format
   * @param response     the response
   */
  public static void writeErrorResponse(int status, String errorMessage, ResponseFormat format,
                                        HttpServletResponse response)
      throws IOException {
    response.setStatus(status);
    PrintWriter writer = response.getWriter();
    switch (format) {
    case JSON:
      Map<String, String> errorMap = new HashMap<String, String>();
      errorMap.put("error", errorMessage);
      writer.write(JsonUtils.toJsonString(errorMap));
      break;
    case XML:
    default:
      writeXmlError(errorMessage, writer);
      break;
    }
  }

  private static void writeXmlError(String errorMessage, Writer out) throws IOException {
    try {
      DocumentBuilderFactory factory = XMLUtils.newSecureDocumentBuilderFactory();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.newDocument();

      Element root = doc.createElement("error");
      root.setTextContent(errorMessage);
      doc.appendChild(root);

      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      Transformer transformer = transformerFactory.newTransformer();
      transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
      transformer.setOutputProperty(OutputKeys.STANDALONE, "no");

      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(out);
      transformer.transform(source, result);
    } catch (ParserConfigurationException | TransformerException e) {
      throw new IOException("Failed to write XML error response", e);
    }
  }

  public enum ResponseFormat {
    UNSPECIFIED("unspecified"),
    JSON("json"),
    XML("xml");
    private final String value;

    ResponseFormat(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return value;
    }

    /**
     * Get Content-Type header value with UTF-8 charset for this format.
     *
     * @return Content-Type string (e.g., "application/json;charset=utf-8"),
     * or null if UNSPECIFIED
     */
    public String getContentType() {
      switch (this) {
      case JSON:
        return MediaType.APPLICATION_JSON_TYPE.withCharset("utf-8").toString();
      case XML:
        return MediaType.APPLICATION_XML_TYPE.withCharset("utf-8").toString();
      default:
        return null;
      }
    }
  }
}
