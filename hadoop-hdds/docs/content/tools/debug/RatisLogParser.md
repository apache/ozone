---
title: "Ratis Log Parser"
date: 2025-07-28
summary: Parse ratis logs to be in human understandable form.
menu: debug
weight: 5
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

The Ratis log parser tool takes a segment file as input and gives a human-readable output.
It can be used to parse Ratis logs from different components by specifying the corresponding role.
```bash
Usage: ozone debug ratis parse [-hV] [--verbose] [--role=<role>] -s=<segmentFile>
Shell for printing Ratis Log in understandable text
  -h, --help          Show this help message and exit.
      --role=<role>   Component role for parsing. Values: om, scm, datanode
                        Default: generic
  -s, --segmentPath, --segment-path=<segmentFile>
                      Path of the segment file
  -V, --version       Print version information and exit.
      --verbose       More verbose output. Show the stack trace of the errors.
```
