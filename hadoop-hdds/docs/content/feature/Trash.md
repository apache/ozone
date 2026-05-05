---
title: Trash
menu:
  main:
    parent: Features
summary: How to use the trash feature in Ozone.
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

The trash feature in Ozone provides a way to recover files that have been accidentally deleted. When enabled, deleted files are moved to a trash directory instead of being permanently removed.

## Enabling the Trash Feature

To enable the trash feature, you need to add the following configuration properties to the Ozone Manager's `core-site.xml`:

```xml
<property>
  <name>fs.trash.interval</name>
  <value>360</value> <!-- Time in minutes -->
</property>

<property>
  <name>fs.trash.classname</name>
  <value>org.apache.hadoop.fs.ozone.OzoneTrashPolicy</value>
</property>
```

The `fs.trash.interval` property specifies the minimum time, in minutes, for which a deleted file will be kept in the trash. After this interval, the trash emptier will permanently delete the file. A value of 0 disables the trash feature.

The `fs.trash.classname` property should be set to `org.apache.hadoop.fs.ozone.OzoneTrashPolicy` to use Ozone's trash implementation.

## Using the Trash

When the trash feature is enabled, any file deleted using the `ozone fs -rm` command will be moved to the trash.

### Trash Location

The trash directory is located at `/<volume>/<bucket>/.Trash/<username>`. For example, if the user `testuser` deletes a file from `/vol1/bucket1`, the file will be moved to `/vol1/bucket1/.Trash/testuser/Current/`.

### Skipping the Trash

To permanently delete a file and bypass the trash, use the `-skipTrash` option with the `rm` command:

```bash
ozone fs -rm -skipTrash /<volume>/<bucket>/<key>
```

### Emptying the Trash

The trash emptier is a background process that periodically deletes files from the trash directory after the configured `fs.trash.interval` has passed. The interval at which the trash emptier runs can be configured with the `ozone.fs.trash.checkpoint.interval` property in `ozone-site.xml`.

```xml
<property>
  <name>ozone.fs.trash.checkpoint.interval</name>
  <value>60</value> <!-- Time in minutes -->
</property>
```

If this property is not set, it defaults to the value of `fs.trash.checkpoint.interval`.
