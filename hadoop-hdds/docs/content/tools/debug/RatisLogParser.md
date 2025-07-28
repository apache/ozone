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

The Ratis log parser tool takes a segment file as input, and give the output in a human understandable format.
It has the following subcommands, which can be used to parse the Ratis logs of different components:

```bash
Usage: ozone debug ratislogparser [-hV] [--verbose] [COMMAND]
Shell of printing Ratis Log in understandable text
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
      --verbose   More verbose output. Show the stack trace of the errors.
Commands:
  datanode  dump datanode segment file
  generic   dump generic ratis segment file
  om        dump om ratis segment file
  scm       dump scm ratis segment file
```
