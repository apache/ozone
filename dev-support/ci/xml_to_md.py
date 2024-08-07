import os
import re
import xml.etree.ElementTree as ET

class Property:
    def __init__(self, name, value, tag, description):
        self.name = name
        self.value = value
        self.tag = tag
        self.description = description

def parse_xml(file_path):
    properties = []
    tree = ET.parse(file_path)
    root = tree.getroot()
    for prop in root.findall('property'):
        name = prop.find('name').text if prop.find('name') is not None else ''
        value = prop.find('value').text if prop.find('value') is not None else ''
        tag = prop.find('tag').text if prop.find('tag') is not None else ''
        description = prop.find('description').text if prop.find('description') is not None else ''
        description = re.sub(r'\s+', ' ', description).strip()
        properties.append(Property(name, value, tag, description))
    return properties

def write_markdown(properties, output_file):
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("<!--\n")
        f.write("Licensed to the Apache Software Foundation (ASF) under one or more\n")
        f.write("contributor license agreements. See the NOTICE file distributed with\n")
        f.write("this work for additional information regarding copyright ownership.\n")
        f.write("The ASF licenses this file to You under the Apache License, Version 2.0\n")
        f.write("(the \"License\"); you may not use this file except in compliance with\n")
        f.write("the License. You may obtain a copy of the License at\n\n")
        f.write("http://www.apache.org/licenses/LICENSE-2.0\n\n")
        f.write("Unless required by applicable law or agreed to in writing, software\n")
        f.write("distributed under the License is distributed on an \"AS IS\" BASIS,\n")
        f.write("WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n")
        f.write("See the License for the specific language governing permissions and\n")
        f.write("limitations under the License.\n")
        f.write("-->\n\n")

        for prop in properties:
            f.write(f"| **Name**        | `{prop.name}` |\n")
            f.write("|:----------------|:----------------------------|\n")
            f.write(f"| **Value**       | {prop.value} |\n")
            f.write(f"| **Tag**         | {prop.tag} |\n")
            f.write(f"| **Description** | {prop.description} |\n")
            f.write("--------------------------------------------------------------------------------\n")

def main():
    properties = {}
    with open('xml_files.txt', 'r') as file_list:
        for file_path in file_list:
            file_path = file_path.strip()
            if os.path.exists(file_path):
                file_properties = parse_xml(file_path)
                for prop in file_properties:
                    properties[prop.name] = prop

    sorted_properties = sorted(properties.values(), key=lambda p: p.name)
    write_markdown(sorted_properties, 'hadoop-hdds/docs/content/tools/Configurations.md')

if __name__ == "__main__":
    main()
