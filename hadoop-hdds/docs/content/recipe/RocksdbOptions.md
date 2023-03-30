---
title: Tuning Rocksdb Options
linktitle: Rocksdb Options
summary: Tuning Rocksdb Options for OM/SCM/DN
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

The rocksdb options used by Ozone are mostly default. This page describes how to use an ini file to customize rocksdb options.

## Use ini file

Place the corresponding `.ini` file in OZONE configuration directory `$OZONE_DIR/etc/hadoop`, and then restart the service.
The following lists the rocksdb `.ini` file loaded by OM/SCM/DN.

Service | filename     
----|---------
OM  | om.db.ini 
SCM | scm.db.ini 
DN  | dn-crl.db.ini

For details about the format of the Rocksdb ini file, see: [rocksdb_option_file_example.ini](https://github.com/facebook/rocksdb/blob/main/examples/rocksdb_option_file_example.ini)

## Verify it takes effect

Check whether the option in `OPTIONS-xxxxxx` file in the rocksdb directory meets expectations.
For example, the OM ini file `$OZONE_DIR/etc/hadoop/om.db.ini` is as follows: (Modify `max_background_flushes=4`)

```ini
[DBOptions]
   max_background_flushes=4
   compaction_readahead_size=4194304
   bytes_per_sync=1048576
   max_background_compactions=4
   max_background_jobs=48
   create_missing_column_families=true
   create_if_missing=true

[CFOptions "default"]

[TableOptions/BlockBasedTable "default"]
```

After OM restarts, check the `OPTIONS-xxxxxx` content in the rocksdb directory (`${DBDATA}/om.db`). The ini conf takes effect if `max_background_flushes=4`.
