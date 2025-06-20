#!/usr/bin/env python3
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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import subprocess
import requests
import json
from robot.api import logger

class BytemanLibrary:
    def __init__(self):
        self.byteman_clients = {}
        
    def connect_to_byteman_agent(self, component_name, host="localhost", port=9091):
        """Connect to Byteman agent on specific component"""
        self.byteman_clients[component_name] = {'host': host, 'port': port, 
                                                'base_url': f"http://{host}:{port}"}
        logger.info(f"Connected to Byteman agent for {component_name} at {host}:{port}")
        
    def add_byteman_rule(self, component_name, rule_file):
        """Add Byteman rule into specific component"""
        client = self.byteman_clients[component_name]
        
        # Use bmsubmit command to load rule
        cmd = ["bmsubmit", f"-p {client['port']}", f"-l {rule_file}"]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to load rule: {result.stderr}")
            
        logger.info(f"Loaded Byteman rule {rule_file} into {component_name}")
        
    def remove_byteman_rule(self, component_name, rule_file):
        """Remove Byteman rule for specific component"""
        client = self.byteman_clients[component_name]
        
        cmd = ["bmsubmit", f"-p {client['port']}", f"-u {rule_file}"]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to unload rule: {result.stderr}")
            
        logger.info(f"Unloaded Byteman rule {rule_file} from {component_name}")
        
    def list_all_byteman_rules(self, component_name):
        """List all Byteman rules for specific component"""
        client = self.byteman_clients[component_name]
        
        cmd = ["bmsubmit", f"-p {client['port']}", "-l"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        logger.info(f"Active rules in {component_name}: {result.stdout}")
        return result.stdout