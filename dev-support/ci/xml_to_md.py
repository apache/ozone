import os
import tarfile
import fnmatch

def extract_tarball(tarball_path, extract_path):
    with tarfile.open(tarball_path, 'r:gz') as tar:
        tar.extractall(path=extract_path)

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
    tarball_path = 'ozone-1.5.0-SNAPSHOT-src.tar.gz'
    extract_path = 'ozone-src/extracted'
    xml_pattern = 'ozone-default-generated.xml'
    output_file = 'hadoop-hdds/docs/content/tools/Configurations.md'

    # Extract the tarball
    extract_tarball(tarball_path, extract_path)

    # Find all XML files
    xml_files = find_xml_files(extract_path, xml_pattern)

    # Parse the XML files and generate Markdown
    properties = parse_xml_files(xml_files)
    generate_markdown(properties, output_file)
