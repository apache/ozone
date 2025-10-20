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

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.hdds.server.http.HttpServer2;
import org.apache.hadoop.hdds.utils.HttpServletUtils;

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

    HttpServletUtils.ResponseFormat format = HttpServletUtils.getResponseFormat(request);
    if (format == HttpServletUtils.ResponseFormat.UNSPECIFIED) {
      // use XML as default response format
      format = HttpServletUtils.ResponseFormat.XML;
    }

    response.setContentType(format.getContentType());

    String name = request.getParameter("name");
    Writer out = response.getWriter();
    String cmd = request.getParameter(COMMAND);

    processCommand(cmd, format, request, response, out, name);
  }

  private void processCommand(String cmd, HttpServletUtils.ResponseFormat format, HttpServletRequest request,
      HttpServletResponse response, Writer out, String name)
      throws IOException {
    try {
      if (cmd == null) {
        writeResponse(getConfFromContext(), out, format, name);
      } else {
        processConfigTagRequest(request, cmd, out);
      }
    } catch (IllegalArgumentException iae) {
      HttpServletUtils.writeErrorResponse(HttpServletResponse.SC_NOT_FOUND, iae.getMessage(), format, response);
    }
  }

  /**
   * Guts of the servlet - extracted for easy testing.
   */
  static void writeResponse(OzoneConfiguration conf, Writer out, HttpServletUtils.ResponseFormat format,
                            String propertyName)
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

  private void processConfigTagRequest(HttpServletRequest request, String cmd, Writer out) throws IOException {
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
