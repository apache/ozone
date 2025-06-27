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
import os
from robot.api import logger

class BytemanLibrary:
    def __init__(self):
        self.byteman_port = os.getenv("BYTEMAN_PORT", "9091")
        self.byteman_cmd = ["bmsubmit", "-p", self.byteman_port]

    def update_component_name(self, component_name):
        return f"{component_name}.org" if component_name in ["scm1", "scm2", "scm3"] else component_name

    def run_byteman_cmd(self, component_name, args, action_desc):
        """Run a byteman command and handle error/logging"""
        cmd = self.byteman_cmd + ["-h", component_name] + args
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise RuntimeError(f"Failed to {action_desc} for {component_name}: {result.stderr.strip()}")
        
        logger.info(f"{action_desc} for {component_name} successful.")
        return result.stdout

    def add_byteman_rule(self, component_name, rule_file):
        """Add Byteman rule into specific component"""
        component_name = self.update_component_name(component_name)
        self.run_byteman_cmd(component_name, ["-l", rule_file], f"Add rule {rule_file}")

    def remove_byteman_rule(self, component_name, rule_file):
        """Remove Byteman rule for specific component"""
        component_name = self.update_component_name(component_name)
        self.run_byteman_cmd(component_name, ["-u", rule_file], f"Remove rule {rule_file}")

    def list_byteman_rules(self, component_name):
        """List Active Byteman rules for specific component and return file list"""
        component_name = self.update_component_name(component_name)
        output = self.run_byteman_cmd(component_name, ["-l"], "List rules")
        
        matching_lines = [line for line in output.splitlines() if '# File' in line]
        file_list = [line.split()[2] for line in matching_lines if len(line.split()) >= 3]

        if matching_lines:
            logger.info(f"Active rules in {component_name}:\n" + "\n".join(matching_lines))
        else:
            logger.info(f"Active rules in {component_name}: No rules found")

        return file_list

    def remove_all_byteman_rules(self, component_name):
        """Remove all Byteman rules for specific component"""
        component_name = self.update_component_name(component_name)
        rule_files = self.list_byteman_rules(component_name)

        if not rule_files:
            logger.info(f"No active rules to remove for {component_name}")
            return

        for rule_file in rule_files:
            self.remove_byteman_rule(component_name, rule_file)
