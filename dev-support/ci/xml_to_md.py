import os
import xml.etree.ElementTree as ET
import tarfile
import fnmatch

class Property:
    def __init__(self, name, value, tag, description):
        self.name = name
        self.value = value
        self.tag = tag
        self.description = description

def parse_xml_file(xml_file):
    properties = {}
    tree = ET.parse(xml_file)
    root = tree.getroot()
    for prop in root.findall('property'):
        name = prop.find('name').text
        value = prop.find('value').text if prop.find('value') is not None else ""
        tag = prop.find('tag').text if prop.find('tag') is not None else ""
        description = prop.find('description').text if prop.find('description') is not None else ""
        properties[name] = Property(name, value, tag, description)
    return properties

def write_markdown(properties, output_file):
    with open(output_file, 'w') as f:
        f.write("<!--\n")
        f.write("Licensed to the Apache Software Foundation (ASF) under one or more\n")
        f.write("contributor license agreements.  See the NOTICE file distributed with\n")
        f.write("this work for additional information regarding copyright ownership.\n")
        f.write("The ASF licenses this file to You under the Apache License, Version 2.0\n")
        f.write("(the \"License\"); you may not use this file except in compliance with\n")
        f.write("the License.  You may obtain a copy of the License at\n\n")
        f.write("   http://www.apache.org/licenses/LICENSE-2.0\n\n")
        f.write("Unless required by applicable law or agreed to in writing, software\n")
        f.write("distributed under the License is distributed on an \"AS IS\" BASIS,\n")
        f.write("WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n")
        f.write("See the License for the specific language governing permissions and\n")
        f.write("limitations under the License.\n")
        f.write("-->\n\n")

        for prop in sorted(properties.values(), key=lambda p: p.name):
            f.write(f"| **Name**        | `{prop.name}` |\n")
            f.write(f"|:----------------|:----------------------------|\n")
            f.write(f"| **Value**       | {prop.value} |\n")
            f.write(f"| **Tag**         | {prop.tag} |\n")
            f.write(f"| **Description** | {prop.description} |\n")
            f.write("--------------------------------------------------------------------------------\n")

def find_xml_files(directory):
    xml_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file == 'ozone-default-generated.xml' and 'test-classes' not in root:
                xml_files.append(os.path.join(root, file))
    return xml_files

def main():
    tar_file = 'hadoop-ozone/dist/target/ozone-*.tar.gz'
    extract_dir = 'ozone-src'

    # Extract the tar file
    with tarfile.open(tar_file, 'r:gz') as tar_ref:
        tar_ref.extractall(extract_dir)

    # Find all XML files
    xml_files = find_xml_files(extract_dir)

    properties = {}
    for xml_file in xml_files:
        properties.update(parse_xml_file(xml_file))

    output_file = "hadoop-hdds/docs/content/tools/Configurations.md"
    write_markdown(properties, output_file)

if __name__ == "__main__":
    main()
