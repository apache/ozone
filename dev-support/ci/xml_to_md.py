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

def find_xml_files(base_path, pattern):
    xml_files = []
    for root, dirs, files in os.walk(base_path):
        for filename in fnmatch.filter(files, pattern):
            if 'test-classes' not in root:
                xml_files.append(os.path.join(root, filename))
    return xml_files

def parse_xml_files(xml_files):
    properties = {}
    for xml_file in xml_files:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        for prop in root.findall('property'):
            name = prop.find('name').text
            value = prop.find('value').text
            if name not in properties:
                properties[name] = value
    return properties

def generate_markdown(properties, output_file):
    with open(output_file, 'w') as f:
        f.write("# Configurations\n\n")
        for name, value in properties.items():
            f.write(f"## {name}\n\n")
            f.write(f"**Value**: {value}\n\n")

if __name__ == "__main__":
    extract_path = 'ozone-src/extracted'
    xml_pattern = 'ozone-default-generated.xml'
    output_file = 'hadoop-hdds/docs/content/tools/Configurations.md'

    # Find all XML files
    xml_files = find_xml_files(extract_path, xml_pattern)

    # Parse the XML files and generate Markdown
    properties = parse_xml_files(xml_files)
    generate_markdown(properties, output_file)
