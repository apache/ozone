/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.conf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.gson.Gson;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.util.XMLUtils;
import org.eclipse.jetty.util.ajax.JSON;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.Mockito;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/** Test for {@link HddsConfServlet}. */
public class TestHddsConfServlet {

  private static final Map<String, String> TEST_PROPERTIES = new HashMap<>();
  private static final Map<String, String> TEST_FORMATS = new HashMap<>();
  private static final String TEST_KEY = "testconfservlet.key";
  private static final String TEST_VAL = "testval";

  @BeforeAll
  public static void setup() {
    TEST_PROPERTIES.put("test.key1", "value1");
    TEST_PROPERTIES.put("test.key2", "value2");
    TEST_PROPERTIES.put("test.key3", "value3");
    TEST_FORMATS.put(HddsConfServlet.FORMAT_XML, "application/xml");
    TEST_FORMATS.put(HddsConfServlet.FORMAT_JSON, "application/json");
  }

  @Test
  public void testParseHeaders() throws Exception {
    HashMap<String, String> verifyMap = new HashMap<String, String>();
    verifyMap.put("text/plain", HddsConfServlet.FORMAT_XML);
    verifyMap.put(null, HddsConfServlet.FORMAT_XML);
    verifyMap.put("text/xml", HddsConfServlet.FORMAT_XML);
    verifyMap.put("application/xml", HddsConfServlet.FORMAT_XML);
    verifyMap.put("application/json", HddsConfServlet.FORMAT_JSON);

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    for (Map.Entry<String, String> entry : verifyMap.entrySet()) {
      String contenTypeActual = entry.getValue();
      Mockito.when(request.getHeader(HttpHeaders.ACCEPT))
          .thenReturn(entry.getKey());
      assertEquals(contenTypeActual,
          HddsConfServlet.parseAcceptHeader(request));
    }
  }

  @Test
  public void testGetProperty() throws Exception {
    OzoneConfiguration conf = getPropertiesConf();
    // list various of property names
    String[] keys = new String[] {"test1.key1",
        "test.unknown.key",
        "",
        "test.key2",
        null};
    for (Map.Entry<String, String> entry : TEST_FORMATS.entrySet()) {
      for (String key : keys) {
        verifyGetProperty(conf, entry.getKey(), key);
      }
    }
  }

  @Test
  public void testGetPropertyWithCmd() throws Exception {
    OzoneConfiguration conf = getPropertiesConf();
    conf.getObject(OzoneTestConfig.class);
    // test cmd is getOzoneTags
    String result = getResultWithCmd(conf, "getOzoneTags");
    Gson gson = new Gson();
    String tags = gson.toJson(OzoneConfiguration.TAGS);
    assertEquals(result, tags);
    // cmd is getPropertyByTag
    result = getResultWithCmd(conf, "getPropertyByTag");
    assertTrue(result.contains("ozone.test.test.key"));
    // cmd is illegal
    getResultWithCmd(conf, "illegal");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteJson() throws Exception {
    StringWriter sw = new StringWriter();
    HddsConfServlet.writeResponse(getTestConf(), sw, "json", null);
    String json = sw.toString();
    boolean foundSetting = false;
    Object parsed = JSON.parse(json);
    Object[] properties = ((Map<String, Object[]>) parsed).get("properties");
    for (Object o : properties) {
      Map<String, Object> propertyInfo = (Map<String, Object>) o;
      String key = (String) propertyInfo.get("key");
      String val = (String) propertyInfo.get("value");
      String resource = (String) propertyInfo.get("resource");
      if (TEST_KEY.equals(key) && TEST_VAL.equals(val)
          && "programmatically".equals(resource)) {
        foundSetting = true;
      }
    }
    assertTrue(foundSetting);
  }

  @Test
  public void testWriteXml() throws Exception {
    StringWriter sw = new StringWriter();
    HddsConfServlet.writeResponse(getTestConf(), sw, "xml", null);
    String xml = sw.toString();

    DocumentBuilderFactory docBuilderFactory =
        XMLUtils.newSecureDocumentBuilderFactory();
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = builder.parse(new InputSource(new StringReader(xml)));
    NodeList nameNodes = doc.getElementsByTagName("name");
    boolean foundSetting = false;
    for (int i = 0; i < nameNodes.getLength(); i++) {
      Node nameNode = nameNodes.item(i);
      String key = nameNode.getTextContent();
      if (TEST_KEY.equals(key)) {
        foundSetting = true;
        Element propertyElem = (Element) nameNode.getParentNode();
        String val = propertyElem.getElementsByTagName("value").
            item(0).getTextContent();
        assertEquals(TEST_VAL, val);
      }
    }
    assertTrue(foundSetting);
  }

  @Test
  public void testBadFormat() throws Exception {
    StringWriter sw = new StringWriter();
    try {
      HddsConfServlet.writeResponse(getTestConf(), sw, "not a format", null);
      fail("writeResponse with bad format didn't throw!");
    } catch (HddsConfServlet.BadFormatException bfe) {
      // expected
    }
    assertEquals("", sw.toString());
  }

  private String getResultWithCmd(OzoneConfiguration conf, String cmd)
      throws Exception {
    StringWriter sw = null;
    PrintWriter pw = null;
    HddsConfServlet service = null;
    try {
      service = new HddsConfServlet();
      ServletConfig servletConf = mock(ServletConfig.class);
      ServletContext context = mock(ServletContext.class);
      service.init(servletConf);
      when(context.getAttribute(HttpServer2.CONF_CONTEXT_ATTRIBUTE))
          .thenReturn(conf);
      when(service.getServletContext()).thenReturn(context);
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getHeader(HttpHeaders.ACCEPT)).
          thenReturn(TEST_FORMATS.get(null));
      when(request.getParameter("cmd")).thenReturn(cmd);
      when(request.getParameter("tags")).thenReturn(ConfigTag.DEBUG.toString());
      HttpServletResponse response = mock(HttpServletResponse.class);
      sw = new StringWriter();
      pw = new PrintWriter(sw);
      when(response.getWriter()).thenReturn(pw);
      // response request
      service.doGet(request, response);
      if (cmd.equals("illegal")) {
        Mockito.verify(response)
            .sendError(
                Mockito.eq(HttpServletResponse.SC_NOT_FOUND),
                Mockito.eq("illegal is not a valid command."));
      }
      String result = sw.toString().trim();
      return result;
    } finally {
      if (sw != null) {
        sw.close();
      }
      if (pw != null) {
        pw.close();
      }
      if (service != null) {
        service.destroy();
      }
    }
  }

  private void verifyGetProperty(OzoneConfiguration conf, String format,
      String propertyName) throws Exception {
    StringWriter sw = null;
    PrintWriter pw = null;
    HddsConfServlet service = null;
    try {
      service = new HddsConfServlet();
      ServletConfig servletConf = mock(ServletConfig.class);
      ServletContext context = mock(ServletContext.class);
      service.init(servletConf);
      when(context.getAttribute(HttpServer2.CONF_CONTEXT_ATTRIBUTE))
          .thenReturn(conf);
      when(service.getServletContext()).thenReturn(context);

      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getHeader(HttpHeaders.ACCEPT)).
          thenReturn(TEST_FORMATS.get(format));
      when(request.getParameter("name")).thenReturn(propertyName);

      HttpServletResponse response = mock(HttpServletResponse.class);
      sw = new StringWriter();
      pw = new PrintWriter(sw);
      when(response.getWriter()).thenReturn(pw);

      // response request
      service.doGet(request, response);
      String result = sw.toString().trim();

      // if property name is null or empty, expect all properties
      // in the response
      if (Strings.isNullOrEmpty(propertyName)) {
        for (Map.Entry<String, String> entry : TEST_PROPERTIES.entrySet()) {
          assertTrue(result.contains(entry.getKey()) &&
                  result.contains(entry.getValue()));
        }
      } else {
        if (conf.get(propertyName) != null) {
          // if property name is not empty and property is found
          assertTrue(result.contains(propertyName));
          for (Map.Entry<String, String> entry : TEST_PROPERTIES.entrySet()) {
            if (!entry.getKey().equals(propertyName)) {
              assertFalse(result.contains(entry.getKey()));
            }
          }
        } else {
          // if property name is not empty, and it's not in configuration
          // expect proper error code and error message is set to the response
          Mockito.verify(response)
              .sendError(
                  Mockito.eq(HttpServletResponse.SC_NOT_FOUND),
                  Mockito.eq("Property " + propertyName + " not found"));
        }
      }
    } finally {
      if (sw != null) {
        sw.close();
      }
      if (pw != null) {
        pw.close();
      }
      if (service != null) {
        service.destroy();
      }
    }
  }

  private OzoneConfiguration getTestConf() {
    OzoneConfiguration testConf = new OzoneConfiguration();
    testConf.set(TEST_KEY, TEST_VAL);
    return testConf;
  }

  private OzoneConfiguration getPropertiesConf() {
    OzoneConfiguration testConf = new OzoneConfiguration();
    for (Map.Entry<String, String> entry : TEST_PROPERTIES.entrySet()) {
      testConf.set(entry.getKey(), entry.getValue());
    }
    return testConf;
  }

  /**
  * Configuration value for test.
  */
  @ConfigGroup(prefix = "ozone.test")
  public static class OzoneTestConfig {
    @Config(
        key = "test.key",
        defaultValue = "value1",
        type = ConfigType.STRING,
        description = "Test get config by tag",
        tags = ConfigTag.DEBUG)
    private String testTag = "value1";
  }
}
