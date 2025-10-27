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
import org.apache.ratis.util.MemoizedCheckedSupplier;
import org.apache.ratis.util.function.CheckedSupplier;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Utility class for HTTP servlet operations.
 * Provides methods for parsing request headers and writing responses.
 */
public final class HttpServletUtils implements Serializable {

  private static final CheckedSupplier<DocumentBuilderFactory, ParserConfigurationException> DOCUMENT_BUILDER_FACTORY =
      MemoizedCheckedSupplier.valueOf(XMLUtils::newSecureDocumentBuilderFactory);

  private HttpServletUtils() {
    // Utility class, prevent instantiation
  }

  /**
   * Get the response format from request header.
   *
   * @param request the HTTP servlet request
   * @return {@link ResponseFormat#JSON} if Accept header contains "application/json",
   * otherwise {@link ResponseFormat#XML} (default for backwards compatibility)
   * @see HttpHeaders#ACCEPT
   */
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
      Map<String, String> errorMap = new HashMap<>();
      errorMap.put("error", errorMessage);
      writer.write(JsonUtils.toJsonString(errorMap));
      break;
    case XML:
      writeXmlError(errorMessage, writer);
      break;
    default:
      throw new IOException("Unsupported response format for error response: " + format,
          new IllegalArgumentException("Bad format: " + format));
    }
  }

  private static void writeXmlError(String errorMessage, Writer out) throws IOException {
    try {
      DocumentBuilder builder = DOCUMENT_BUILDER_FACTORY.get().newDocumentBuilder();
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

  /**
   * Write response according to the specified format.
   * The caller provides a callback to write the content.
   *
   * @param response        the HTTP servlet response
   * @param format          the response format
   * @param contentWriter   callback to write content to the writer
   * @param exceptionClass  class of exception to propagate from contentWriter
   * @param <E>             the type of exception that may be thrown by contentWriter
   * @throws IOException if an I/O error occurs
   * @throws E           if contentWriter throws an exception of type E
   */
  public static <E extends Exception> void writeResponse(HttpServletResponse response, ResponseFormat format,
      CheckedConsumer<Writer> contentWriter, Class<E> exceptionClass) throws IOException, E {
    response.setContentType(format.getContentType());
    Writer out = response.getWriter();
    try {
      contentWriter.accept(out);
    } catch (IOException e) {
      // Always rethrow IOException as-is
      throw e;
    } catch (Exception e) {
      // If exception matches the generic type, throw it
      if (exceptionClass.isInstance(e)) {
        throw exceptionClass.cast(e);
      }
      // Otherwise wrap in IOException
      throw new IOException("Failed to write response", e);
    }
  }

  /**
   * Functional interface for operations that accept a parameter and may throw exceptions.
   *
   * @param <T> the type of the input to the operation
   */
  @FunctionalInterface
  public interface CheckedConsumer<T> {
    void accept(T t) throws Exception;
  }

  /**
   * Response format enumeration for HTTP responses.
   * Supports JSON, XML, and UNSPECIFIED formats.
   */
  public enum ResponseFormat {
    UNSPECIFIED("unspecified"),
    JSON("json"),
    XML("xml");
    private final String value;

    ResponseFormat(String value) {
      this.value = value;
    }

    /**
     * Get the string value of this response format.
     *
     * @return the format value (e.g., "json", "xml", "unspecified")
     */
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
