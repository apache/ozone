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

package org.apache.hadoop.hdds.conf;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.hdds.server.http.HttpServer2;
import org.apache.hadoop.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * A servlet to print out the running configuration data.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class HddsConfServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  private static final String COMMAND = "cmd";
  private static final OzoneConfiguration OZONE_CONFIG =
      new OzoneConfiguration();

  /**
   * Return the Configuration of the daemon hosting this servlet.
   * This is populated when the HttpServer starts.
   */
  private OzoneConfiguration getConfFromContext() {
    OzoneConfiguration conf =
        (OzoneConfiguration) getServletContext().getAttribute(
            HttpServer2.CONF_CONTEXT_ATTRIBUTE);
    assert conf != null;
    return conf;
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    if (!HttpServer2.isInstrumentationAccessAllowed(getServletContext(),
        request, response)) {
      return;
    }

    ResponseFormat format = parseAcceptHeader(request);
    switch (format) {
    case JSON:
      response.setContentType("application/json; charset=utf-8");
      break;
    case XML:
    default:
      response.setContentType("text/xml; charset=utf-8");
      break;
    }

    String name = request.getParameter("name");
    String cmd = request.getParameter(COMMAND);

    try (Writer out = response.getWriter()) {
      processCommand(cmd, format, request, response, out, name);
    }
  }

  private void processCommand(String cmd, ResponseFormat format,
                              HttpServletRequest request, HttpServletResponse response, Writer out,
                              String name)
      throws IOException {
    try {
      if (cmd == null) {
        writeResponse(getConfFromContext(), out, format, name);
      } else {
        processConfigTagRequest(request, cmd, out);
      }
    } catch (IllegalArgumentException iae) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      writeErrorResponse(iae.getMessage(), format, out);
    }
  }

  /**
   * Parse the Accept header to determine response format.
   *
   * @param request the HTTP servlet request
   * @return {@link ResponseFormat#JSON} if Accept header contains "application/json",
   * otherwise {@link ResponseFormat#XML} (default for backwards compatibility)
   * @see HttpHeaders#ACCEPT
   */
  @VisibleForTesting
  static ResponseFormat parseAcceptHeader(HttpServletRequest request) {
    String format = request.getHeader(HttpHeaders.ACCEPT);
    return format != null && format.contains(ResponseFormat.JSON.getValue()) ?
        ResponseFormat.JSON : ResponseFormat.XML;
  }

  /**
   * Guts of the servlet - extracted for easy testing.
   */
  static void writeResponse(OzoneConfiguration conf,
                            Writer out, ResponseFormat format, String propertyName)
      throws IOException, IllegalArgumentException {
    switch (format) {
    case JSON:
      OzoneConfiguration.dumpConfiguration(conf, propertyName, out);
      break;
    case XML:
    default:
      conf.writeXml(propertyName, out);
      break;
    }
  }

  /**
   * Write error response according to the specified format.
   *
   * @param errorMessage the error message
   * @param format       the response format
   * @param out          the writer
   */
  static void writeErrorResponse(String errorMessage, ResponseFormat format, Writer out)
      throws IOException {
    switch (format) {
    case JSON:
      Map<String, String> errorMap = new HashMap<>();
      errorMap.put("error", errorMessage);
      out.write(JsonUtils.toJsonString(errorMap));
      break;
    case XML:
    default:
      writeXmlError(errorMessage, out);
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

  enum ResponseFormat {
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
  }

  /**
   * Exception for signal bad content type.
   */
  public static class BadFormatException extends Exception {

    private static final long serialVersionUID = 1L;

    public BadFormatException(String msg) {
      super(msg);
    }
  }

  private void processConfigTagRequest(HttpServletRequest request, String cmd,
                                       Writer out) throws IOException {
    OzoneConfiguration config = getOzoneConfig();

    switch (cmd) {
    case "getOzoneTags":
      out.write(JsonUtils.toJsonString(OzoneConfiguration.TAGS));
      break;
    case "getPropertyByTag":
      String tags = request.getParameter("tags");
      if (tags == null || tags.isEmpty()) {
        throw new IllegalArgumentException("The tags parameter should be set" +
            " when using the getPropertyByTag command.");
      }
      Map<String, Properties> propMap = new HashMap<>();

      for (String tag : tags.split(",")) {
        if (config.isPropertyTag(tag)) {
          Properties properties = config.getAllPropertiesByTag(tag);
          propMap.put(tag, properties);
        }
      }
      out.write(JsonUtils.toJsonString(propMap));
      break;
    default:
      throw new IllegalArgumentException(cmd + " is not a valid command.");
    }

  }

  private static OzoneConfiguration getOzoneConfig() {
    return OZONE_CONFIG;
  }
}
