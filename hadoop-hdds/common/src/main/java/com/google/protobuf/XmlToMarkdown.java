package com.google.protobuf;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileWriter;
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
            for (Property prop : properties) {
                markdown.append("| **Name**        | `").append(prop.name).append("` |\n");
                markdown.append("|:----------------|:----------------------------|\n");
                markdown.append("| **Value**       | ").append(prop.value).append(" |\n");
                markdown.append("| **Tag**         | ").append(prop.tag).append(" |\n");
                markdown.append("| **Description** | ").append(prop.description).append(" |\n");
                markdown.append("--------------------------------------------------------------------------------\n");
            }

            // Write the markdown to a file
            try (FileWriter fileWriter = new FileWriter("hadoop-hdds/docs/content/tools/Configurations.md")) {
                fileWriter.write(markdown.toString());
            }

        } catch (Exception e) {
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
