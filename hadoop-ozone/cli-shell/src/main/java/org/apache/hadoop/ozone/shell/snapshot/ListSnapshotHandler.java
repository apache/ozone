/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.shell.snapshot;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneSnapshot;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.ListPaginationOptions;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.PrefixFilterOption;
import org.apache.hadoop.ozone.shell.bucket.BucketUri;
import picocli.CommandLine;

/**
 * ozone sh snapshot list.
 * a handler for Ozone shell CLI command 'list snapshot'.
 */
@CommandLine.Command(name = "list",
    aliases = "ls",
    description = "List snapshots for the buckets.")
public class ListSnapshotHandler extends Handler {

  @CommandLine.Mixin
  private BucketUri snapshotPath;

  @CommandLine.Mixin
  private ListPaginationOptions listOptions;

  @CommandLine.Mixin
  private PrefixFilterOption prefixFilter;

  @Override
  protected OzoneAddress getAddress() {
    return snapshotPath.getValue();
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {
    String volumeName = snapshotPath.getValue().getVolumeName();
    String bucketName = snapshotPath.getValue().getBucketName();

    Iterator<OzoneSnapshot> snapshotInfos = client.getObjectStore()
        .listSnapshot(volumeName, bucketName, prefixFilter.getPrefix(),
            listOptions.getStartItem());
    int counter = printAsJsonArray(snapshotInfos, listOptions.getLimit());
    if (isVerbose()) {
      err().printf("Found : %d snapshots for o3://%s/%s %n", counter,
          volumeName, bucketName);
    }
  }
}
