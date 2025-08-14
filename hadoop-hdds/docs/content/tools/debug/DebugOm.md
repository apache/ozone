---
title: "Debug OM"
date: 2025-07-28
summary: Debug commands related to OM.
menu: debug
weight: 2
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

Debug commands related to OM.
It has the following subcommands:
## generate-compaction-dag 
Creates a DAG image of the current compaction log of an om.db instance. It is downloaded to the specified location.
```bash
Usage: ozone debug om generate-compaction-dag [-hV] [--verbose] --db=<dbPath>
       -o=<imageLocation>
Create an image of the current compaction log DAG. This command is an offline
command. i.e., it can run on any instance of om.db and does not require OM to
be up.
      --db=<dbPath>   Path to OM RocksDB
  -h, --help          Show this help message and exit.
  -o, --output-file=<imageLocation>
                      Path to location at which image will be downloaded.
                        Should include the image file name with ".png"
                        extension.
  -V, --version       Print version information and exit.
      --verbose       More verbose output. Show the stack trace of the errors.
```

## prefix
Parses the contents of a prefix.
```bash
Usage: ozone debug om prefix [--verbose] --bucket=<bucket> --db=<dbPath>
                             --path=<filePath> --volume=<volume>
Parse prefix contents
      --bucket=<bucket>   bucket name
      --db=<dbPath>       Path to OM RocksDB
      --path=<filePath>   prefixFile Path
      --verbose           More verbose output. Show the stack trace of the
                            errors.
      --volume=<volume>   volume name
```
