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

import static org.apache.hadoop.hdds.conf.OzoneConfiguration.getConfigurationResourceFiles;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.ConfServlet.BadFormatException;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.hdds.server.http.HttpServer2;
import org.apache.hadoop.hdds.utils.HttpServletUtils;
import org.apache.hadoop.util.XMLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * A servlet to print out the running configuration data.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class HddsConfServlet extends HttpServlet {
  private static final Logger LOG =
      LoggerFactory.getLogger(HddsConfServlet.class);

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

    HttpServletUtils.ResponseFormat format = HttpServletUtils.getResponseFormat(request);
    if (format == HttpServletUtils.ResponseFormat.UNSPECIFIED) {
      // use XML as default response format
      format = HttpServletUtils.ResponseFormat.XML;
    }

    String name = request.getParameter("name");
    String cmd = request.getParameter(COMMAND);

    processCommand(cmd, format, request, response, name);
  }

  private void processCommand(String cmd, HttpServletUtils.ResponseFormat format, HttpServletRequest request,
                              HttpServletResponse response, String name)
      throws IOException {
    try {
      if (cmd == null) {
        HttpServletUtils.writeResponse(response, format, (out) -> {
          switch (format) {
          case JSON:
            OzoneConfiguration.dumpConfiguration(getConfFromContext(), name, out);
            break;
          case XML:
            getConfFromContext().writeXml(name, out);
            break;
          default:
            throw new BadFormatException("Bad format: " + format);
          }
        }, IllegalArgumentException.class);
      } else {
        processConfigTagRequest(request, cmd, response);
      }
    } catch (IllegalArgumentException iae) {
      HttpServletUtils.writeErrorResponse(HttpServletResponse.SC_NOT_FOUND, iae.getMessage(), format, response);
    }
  }

  private void processConfigTagRequest(HttpServletRequest request, String cmd, HttpServletResponse response)
      throws IOException {
    OzoneConfiguration config = getOzoneConfig();
    Writer out = response.getWriter();

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

      Map<String, String> descriptionMap = buildDescriptionMap(config);
      Map<String, Map<String, OzoneConfiguration.Property>> propMap = new HashMap<>();

      for (String tag : tags.split(",")) {
        if (config.isPropertyTag(tag)) {
          Properties properties = config.getAllPropertiesByTag(tag);
          Map<String, OzoneConfiguration.Property> metadataMap = new HashMap<>();

          for (String propName : properties.stringPropertyNames()) {
            String value = properties.getProperty(propName);
            String description = descriptionMap.getOrDefault(propName, "");
            OzoneConfiguration.Property property = new OzoneConfiguration.Property();
            property.setName(propName);
            property.setValue(value);
            property.setDescription(description);
            metadataMap.put(propName, property);
          }
          propMap.put(tag, metadataMap);
        }
      }
      out.write(JsonUtils.toJsonString(propMap));
      break;
    default:
      throw new IllegalArgumentException(cmd + " is not a valid command.");
    }
  }

  /**
   * Build a map of property names to descriptions by reading from configuration resources.
   * @param config the OzoneConfiguration to extract descriptions from
   * @return map of property name to description
   */
  private Map<String, String> buildDescriptionMap(OzoneConfiguration config) {
    Map<String, String> descriptionMap = new HashMap<>();

    try {
      DocumentBuilderFactory factory = XMLUtils.newSecureDocumentBuilderFactory();
      DocumentBuilder builder = factory.newDocumentBuilder();

      for (String resourceName : getConfigurationResourceFiles()) {
        URL resourceUrl = config.getResource(resourceName);
        if (resourceUrl != null) {
          parseXmlDescriptions(builder, resourceUrl, descriptionMap);
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to parse XML resource files", e);
    }

    return descriptionMap;
  }

  /**
   * Parse XML configuration file and extract property descriptions using DOM parser.
   * @param builder The XML parser
   * @param resourceUrl URL of the XML resource to parse
   * @param descriptionMap map to populate with property name -> description mappings
   */
  private void parseXmlDescriptions(DocumentBuilder builder, URL resourceUrl, Map<String, String> descriptionMap) {
    try (InputStream inputStream = resourceUrl.openStream()) {
      Document doc = builder.parse(inputStream);
      NodeList propertyNodes = doc.getElementsByTagName("property");

      for (int i = 0; i < propertyNodes.getLength(); i++) {
        Node propertyNode = propertyNodes.item(i);
        if (propertyNode.getNodeType() == Node.ELEMENT_NODE) {
          Element propertyElement = (Element) propertyNode;

          String name = getTextContent(propertyElement, "name");
          String description = getTextContent(propertyElement, "description");

          if (name != null && !StringUtils.isBlank(description)) {
            descriptionMap.put(name, description.trim());
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to parse XML from resource: {}", resourceUrl, e);
    }
  }

  /**
   * Get text content of a child element by tag name.
   * @param parent parent element
   * @param tagName tag name of child element
   * @return text content of the child element, or null if not found
   */
  private String getTextContent(Element parent, String tagName) {
    NodeList nodeList = parent.getElementsByTagName(tagName);
    if (nodeList.getLength() > 0) {
      Node node = nodeList.item(0);
      return node.getTextContent();
    }
    return null;
  }

  private static OzoneConfiguration getOzoneConfig() {
    return OZONE_CONFIG;
  }
}
