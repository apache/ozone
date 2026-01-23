#!/usr/bin/python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Python file to convert XML properties into Markdown
import os
import re
import zipfile
import xml.etree.ElementTree as ET
from collections import namedtuple
from pathlib import Path
import sys

Property = namedtuple('Property', ['name', 'value', 'tag', 'description'])

def extract_xml_from_jar(jar_path, xml_filename):
  xml_files = []
  with zipfile.ZipFile(jar_path, 'r') as jar:
    for file_info in jar.infolist():
      if file_info.filename.endswith(xml_filename):
        with jar.open(file_info.filename) as xml_file:
          xml_files.append(xml_file.read())
  return xml_files

def wrap_config_keys_in_description(description, properties):
  for key in properties.keys():
    description = re.sub(r'\b' + re.escape(key) + r'\b', f'`{key}`', description)
  return description

def parse_xml_file(xml_content, properties):
  root = ET.fromstring(xml_content)
  for prop in root.findall('property'):
    name = prop.findtext('name')
    if not name:
      raise ValueError("Property 'name' is required but missing in XML.")
    description = prop.findtext('description', '')
    if not description:
      raise ValueError(f"Property '{name}' is missing a description.")
    tag = prop.findtext('tag', '')
    if tag:
      formatted_tag = '<br/>'.join(f'`{t.strip()}`' for t in tag.split(','))
    else:
      formatted_tag = ''
    
    properties[name] = Property(
      name=name,
      value=prop.findtext('value', ''),
      tag=formatted_tag,
      description=wrap_config_keys_in_description(
        ' '.join(description.split()).strip(),
        properties
      )
    )
  return properties

def generate_markdown(properties):
  markdown = f"""
## Ozone Configuration Keys
This page provides da comprehensive overview of the configuration keys available in Ozone.
### Configuration Keys
"""

  for prop in sorted(properties.values(), key=lambda p: p.name):
    markdown += f"""
| **Name**        | `{prop.name}` |
|:----------------|:----------------------------|
| **Value**       | {prop.value} |
| **Tag**         | {prop.tag} |
| **Description** | {prop.description} |
--------------------------------------------------------------------------------
"""
  return markdown

def main():
  if len(sys.argv) < 2 or len(sys.argv) > 3:
    print("Usage: python3 xml_to_md.py <base_path> [<output_path>]")
    sys.exit(1)

  base_path = sys.argv[1]
  output_path = sys.argv[2] if len(sys.argv) == 3 else None

  # Find ozone SNAPSHOT directory dynamically using regex
  snapshot_dir = next(
    (os.path.join(base_path, d) for d in os.listdir(base_path) if re.match(r'ozone-[\d.]+\d-SNAPSHOT', d)),
    None
  )

  if not snapshot_dir:
    raise ValueError("SNAPSHOT directory not found in the specified base path.")

  extract_path = os.path.join(snapshot_dir, 'share', 'ozone', 'lib')
  xml_filename = 'ozone-default.xml'

  property_map = {}
  for file_name in os.listdir(extract_path):
    if file_name.endswith('.jar'):
      jar_path = os.path.join(extract_path, file_name)
      xml_contents = extract_xml_from_jar(jar_path, xml_filename)
      for xml_content in xml_contents:
        parse_xml_file(xml_content, property_map)

  markdown_content = generate_markdown(property_map)

  if output_path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open('w', encoding='utf-8') as file:
      file.write(markdown_content)
  else:
    print(markdown_content)

if __name__ == '__main__':
  main()
