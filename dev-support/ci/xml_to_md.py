import os
import re
import zipfile
import xml.etree.ElementTree as ET
from collections import namedtuple
from pathlib import Path

Property = namedtuple('Property', ['name', 'value', 'tag', 'description'])

def extract_xml_from_jar(jar_path, xml_filename):
    xml_files = []
    with zipfile.ZipFile(jar_path, 'r') as jar:
        for file_info in jar.infolist():
            if file_info.filename.endswith(xml_filename):
                with jar.open(file_info.filename) as xml_file:
                    xml_files.append(xml_file.read())
    return xml_files

def parse_xml_file(xml_content):
    root = ET.fromstring(xml_content)
    properties = {}
    for prop in root.findall('property'):
        name = prop.find('name').text if prop.find('name') is not None else ''
        value = prop.find('value').text if prop.find('value') is not None else ''
        tag = prop.find('tag').text if prop.find('tag') is not None else ''
        description = prop.find('description').text if prop.find('description') is not None else ''
        description = ' '.join(description.split()).strip() if description else ''
        properties[name] = Property(name, value, tag, description)
    return properties

def generate_markdown(properties):
    markdown = []
    markdown.append("---\n")
    markdown.append("title: \"Ozone configurations\"\n")
    markdown.append("summary: Ozone configurations\n")
    markdown.append("---\n")
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
    base_path = 'ozone-bin/extracted'

    # Find ozone SNAPSHOT directory dynamically using regex
    snapshot_dir = next(
        (os.path.join(base_path, d) for d in os.listdir(base_path) if re.match(r'ozone-.*-SNAPSHOT', d)),
        None
    )

    extract_path = os.path.join(snapshot_dir, 'share', 'ozone', 'lib')
    xml_filename = 'ozone-default-generated.xml'

    property_map = {}
    for file_name in os.listdir(extract_path):
        if file_name.endswith('.jar'):
            jar_path = os.path.join(extract_path, file_name)
            xml_contents = extract_xml_from_jar(jar_path, xml_filename)
            for xml_content in xml_contents:
                property_map.update(parse_xml_file(xml_content))

    markdown_content = generate_markdown(property_map)
    output_path = Path("hadoop-hdds/docs/content/tools/Configurations.md")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open('w', encoding='utf-8') as file:
        file.write(markdown_content)

if __name__ == '__main__':
    main()