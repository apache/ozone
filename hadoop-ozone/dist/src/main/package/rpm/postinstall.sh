#!/usr/bin/env bash
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

set -e

# Java version check
has_compatible_java() {
    local java_packages=(
        "java-1.8.0-openjdk"
        "java-11-openjdk" 
        "java-17-openjdk"
        "java-21-openjdk"
        "java-1.8.0-openjdk-headless"
        "java-11-openjdk-headless"
        "java-17-openjdk-headless"
        "java-21-openjdk-headless"
    )
    
    for pkg in "${java_packages[@]}"; do
        if rpm -q "$pkg" >/dev/null 2>&1; then
            echo "Found Java package: $pkg"
            return 0
        fi
    done
    
    if command -v java >/dev/null 2>&1; then
        local java_version
        java_version=$(java -version 2>&1 | grep -E "version|openjdk" | head -1)
        
        if echo "$java_version" | grep -E "(1\.[8-9]\.|[8-9]\.|[1-9][0-9]+\.)" >/dev/null; then
            echo "Found compatible Java runtime: $java_version"
            return 0
        fi
    fi
    
    return 1
}

# Main
if has_compatible_java; then
    echo "Compatible OpenJDK (>= 8) already installed, no action needed"
else
    echo "No compatible OpenJDK found"
fi
