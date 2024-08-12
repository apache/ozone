# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import fnmatch
import xml.etree.ElementTree as ET
from collections import namedtuple
from pathlib import Path

Property = namedtuple('Property', ['name', 'value', 'tag', 'description'])

def find_xml_files(base_path, pattern):
    xml_files = []
    for root, dirs, files in os.walk(base_path):
        for filename in fnmatch.filter(files, pattern):
            if 'test-classes' not in root:
                xml_files.append(os.path.join(root, filename))
    return xml_files

def parse_xml_file(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    properties = {}
    for prop in root.findall('property'):
        name = prop.find('name').text if prop.find('name') is not None else ''
        value = prop.find('value').text if prop.find('value') is not None else ''
        tag = prop.find('tag').text if prop.find('tag') is not None else ''
        description = prop.find('description').text if prop.find('description') is not None else ''
        description = ' '.join(description.split()).strip
        properties[name] = Property(name, value, tag, description)
    return properties

def generate_markdown(properties):
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
    extract_path = 'ozone-src/extracted'
    xml_pattern = 'ozone-default-generated.xml'
    xml_files = find_xml_files(extract_path, xml_pattern)

    property_map = {}
    for xml_file in xml_files:
        property_map.update(parse_xml_file(xml_file))

    markdown_content = generate_markdown(property_map)
    output_path = Path("hadoop-hdds/docs/content/tools/Configurations.md")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open('w', encoding='utf-8') as file:
        file.write(markdown_content)

if __name__ == '__main__':
    main()
