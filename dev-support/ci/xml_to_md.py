import os
import xml.etree.ElementTree as ET
from collections import namedtuple
from pathlib import Path

# Define the Property namedtuple to hold property details
Property = namedtuple('Property', ['name', 'value', 'tag', 'description'])

def parse_xml_file(file_path):
    """
    Parse the given XML file and extract properties.

    :param file_path: Path to the XML file
    :return: Dictionary of properties with property names as keys
    """
    tree = ET.parse(file_path)
    root = tree.getroot()
    properties = {}
    for prop in root.findall('property'):
        name = prop.find('name').text if prop.find('name') is not None else ''
        value = prop.find('value').text if prop.find('value') is not None else ''
        tag = prop.find('tag').text if prop.find('tag') is not None else ''
        description = prop.find('description').text if prop.find('description') is not None else ''
        description = ' '.join(description.split()).strip()  # Clean up whitespace
        properties[name] = Property(name, value, tag, description)
    return properties

def generate_markdown(properties):
    """
    Generate Markdown content from properties.

    :param properties: Dictionary of properties
    :return: Markdown string
    """
    markdown = []
    markdown.append("<!--\n")
    markdown.append("Licensed to the Apache Software Foundation (ASF) under one or more\n")
    markdown.append("contributor license agreements.  See the NOTICE file distributed with\n")
    markdown.append("this work for additional information regarding copyright ownership.\n")
    markdown.append("The ASF licenses this file to You under the Apache License, Version 2.0\n")
    markdown.append("(the \"License\"); you may not use this file except in compliance with\n")
    markdown.append("the License.  You may obtain a copy of the License at\n\n")
    markdown.append("   http://www.apache.org/licenses/LICENSE-2.0\n\n")
    markdown.append("Unless required by applicable law or agreed to in writing, software\n")
    markdown.append("distributed under the License is distributed on an \"AS IS\" BASIS,\n")
    markdown.append("WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n")
    markdown.append("See the License for the specific language governing permissions and\n")
    markdown.append("limitations under the License.\n")
    markdown.append("-->\n\n")

    for prop in sorted(properties.values(), key=lambda p: p.name):
        markdown.append(f"| **Name**        | `{prop.name}` |\n")
        markdown.append("|:----------------|:----------------------------|\n")
        markdown.append(f"| **Value**       | {prop.value} |\n")
        markdown.append(f"| **Tag**         | {prop.tag} |\n")
        markdown.append(f"| **Description** | {prop.description} |\n")
        markdown.append("--------------------------------------------------------------------------------\n")

    return ''.join(markdown)

def main():
    """
    Main function to parse XML files and generate Markdown documentation.
    """
    xml_files = [
        "hadoop-hdds/client/target/classes/ozone-default-generated.xml",
        "hadoop-hdds/common/target/classes/ozone-default-generated.xml",
        "hadoop-hdds/container-service/target/classes/ozone-default-generated.xml",
        "hadoop-hdds/framework/target/classes/ozone-default-generated.xml",
        "hadoop-hdds/server-scm/target/classes/ozone-default-generated.xml",
        "hadoop-ozone/common/target/classes/ozone-default-generated.xml",
        "hadoop-ozone/csi/target/classes/ozone-default-generated.xml",
        "hadoop-ozone/ozone-manager/target/classes/ozone-default-generated.xml",
        "hadoop-ozone/recon-codegen/target/classes/ozone-default-generated.xml",
        "hadoop-ozone/recon/target/classes/ozone-default-generated.xml",
    ]

    property_map = {}

    for xml_file in xml_files:
        if not os.path.exists(xml_file):
            print(f"File not found: {xml_file}")
            continue

        properties = parse_xml_file(xml_file)
        property_map.update(properties)

    markdown_content = generate_markdown(property_map)

    output_path = Path("hadoop-hdds/docs/content/tools/Configurations.md")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open('w', encoding='utf-8') as file:
        file.write(markdown_content)

if __name__ == '__main__':
    main()
