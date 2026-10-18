#!/usr/bin/env python3
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

import urllib.request
import json
import sys
import socket

def is_running(host, port):
    try:
        with socket.create_connection((host, int(port)), timeout=2):
            return True
    except Exception:
        return False

def main():
    try:
        res = urllib.request.urlopen("http://prometheus:9090/api/v1/targets")
        data = json.loads(res.read().decode())
        targets = data.get("data", {}).get("activeTargets", [])
        if not targets:
            print("No active targets found in Prometheus")
            sys.exit(1)
            
        failed = False
        checked = 0
        for t in targets:
            url = t.get("scrapeUrl", "")
            # scrapeUrl is like "http://scm:9876/prom"
            try:
                host_port = url.split("//")[1].split("/")[0]
                if ":" in host_port:
                    host, port = host_port.split(":")
                else:
                    host = host_port
                    port = 80
            except Exception:
                continue
                
            if is_running(host, port):
                checked += 1
                health = t.get("health", "")
                print(f"Target {host}:{port} is running. Prometheus health: {health}")
                if health != "up":
                    print(f"Error: Target {host}:{port} is running but Prometheus health is '{health}'. Last error: {t.get('lastError')}")
                    failed = True
            else:
                print(f"Target {host}:{port} is not running. Skipping check.")
                
        if checked == 0:
            print("Error: No running targets were checked!")
            sys.exit(1)
            
        if failed:
            sys.exit(1)
            
        print(f"Successfully verified {checked} running targets.")
    except Exception as e:
        print(f"Exception during health check: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
