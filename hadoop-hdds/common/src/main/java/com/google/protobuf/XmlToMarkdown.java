/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.google.protobuf;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

class Property {
  String name;
  String value;
  String tag;
  String description;

  Property(String name, String value, String tag, String description) {
    this.name = name;
    this.value = value;
    this.tag = tag;
    this.description = description;
  }
}

public class XmlToMarkdown {

  public static void main(String[] args) {
    try {
      // List of XML files to process
      String[] xmlFiles = {"hadoop-hdds/common/src/main/resources/ozone-default.xml",
              "hadoop-hdds/config/target/test-classes/ozone-default-generated.xml"};

      List<Property> properties = new ArrayList<>();

      // Load and parse each XML file
      for (String xmlFile : xmlFiles) {
        File inputFile = new File(xmlFile);
        if (!inputFile.exists()) {
          System.out.println("File not found: " + xmlFile);
          continue;
        }

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(inputFile);
        doc.getDocumentElement().normalize();

        // Get all properties
        NodeList nList = doc.getElementsByTagName("property");

        // Process each property
        for (int temp = 0; temp < nList.getLength(); temp++) {
          Node nNode = nList.item(temp);
          if (nNode.getNodeType() == Node.ELEMENT_NODE) {
            Element eElement = (Element) nNode;
            String name = getTextContent(eElement, "name");
            String value = getTextContent(eElement, "value");
            String tag = getTextContent(eElement, "tag");
            String description = getTextContent(eElement, "description").replaceAll("\\s+", " ").trim();

            properties.add(new Property(name, value, tag, description));
          }
        }
      }

      // Sort properties by name
      properties.sort(Comparator.comparing(p -> p.name));

      // Generate markdown content
      StringBuilder markdown = new StringBuilder();
      // Add Apache license header
      markdown.append("<!--\n")
        .append("Licensed to the Apache Software Foundation (ASF) under one or more\n")
        .append("contributor license agreements.  See the NOTICE file distributed with\n")
        .append("this work for additional information regarding copyright ownership.\n")
        .append("The ASF licenses this file to You under the Apache License, Version 2.0\n")
        .append("(the \"License\"); you may not use this file except in compliance with\n")
        .append("the License.  You may obtain a copy of the License at\n\n")
        .append("   http://www.apache.org/licenses/LICENSE-2.0\n\n")
        .append("Unless required by applicable law or agreed to in writing, software\n")
        .append("distributed under the License is distributed on an \"AS IS\" BASIS,\n")
        .append("WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n")
        .append("See the License for the specific language governing permissions and\n")
        .append("limitations under the License.\n")
        .append("-->\n\n");

      // Add properties
      for (Property prop : properties) {
        markdown.append("| **Name**        | `").append(prop.name).append("` |\n");
        markdown.append("|:----------------|:----------------------------|\n");
        markdown.append("| **Value**       | ").append(prop.value).append(" |\n");
        markdown.append("| **Tag**         | ").append(prop.tag).append(" |\n");
        markdown.append("| **Description** | ").append(prop.description).append(" |\n");
        markdown.append("--------------------------------------------------------------------------------\n");
      }

      // Write the markdown to a file
      Path outputPath = Paths.get("hadoop-hdds/docs/content/tools/Configurations.md");
      try (Writer fileWriter = new OutputStreamWriter(Files.newOutputStream(outputPath), StandardCharsets.UTF_8)) {
        fileWriter.write(markdown.toString());
      }

    } catch (ParserConfigurationException | SAXException | IOException e) {
      e.printStackTrace();
    }
  }

  private static String getTextContent(Element element, String tagName) {
    NodeList nodeList = element.getElementsByTagName(tagName);
    if (nodeList.getLength() > 0 && nodeList.item(0) != null) {
      return nodeList.item(0).getTextContent();
    }
    return ""; // Return an empty string if the element is missing
  }
}
