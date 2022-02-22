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

import sys
import os
import re

# The first argument to the script is the directory where the documentation is
# stored.
docs_directory = os.path.expanduser(sys.argv[1])
content_directory = os.path.join(docs_directory, 'content')

for root, subdirs, files in os.walk(docs_directory):
    for filename in files:
        # We only want to modify markdown files.
        if filename.endswith('.md'):
            file_path = os.path.join(root, filename)

            new_file_content = []

            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    # If the line contains the image tag, we need to replace it
                    if re.search(re.compile("^!\[(.*?)\]\((.*?)\)"), line):
                        print('file %s (full path: %s)' %
                              (filename, file_path))
                        print(f"found markdown image: {line}", end="")

                        line_replacement = line.replace(
                            '![', '{{< image alt="').replace('](', '" src="').replace(')', '">}}')

                        print(
                            f"replaced with shortcode: {line_replacement}")

                        new_file_content.append(line_replacement)

                    else:
                        new_file_content.append(line)

            with open(file_path, 'w', encoding='utf-8') as f:
                f.writelines(new_file_content)
