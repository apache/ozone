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
import org.apache.hadoop.ozone.client.OzoneSnapshotDiff;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.ListPaginationOptions;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.bucket.BucketUri;
import picocli.CommandLine;

/**
 * ozone sh snapshot listDiff.
 */
@CommandLine.Command(name = "listDiff",
    aliases = {"listDiffJob", "lsDiff", "lsDiffJob"},
    description = "List snapshotDiff jobs for a bucket.")
public class ListSnapshotDiffHandler extends Handler {

  @CommandLine.Mixin
  private BucketUri snapshotPath;

  @CommandLine.Option(names = {"--job-status"},
      description = "List jobs based on status.\n" +
      "Accepted values are: queued, in_progress, done, failed, rejected",
      defaultValue = "in_progress")
  private String jobStatus;

  @CommandLine.Option(names = {"--all-status"},
      description = "List all jobs regardless of status.",
      defaultValue = "false")
  private boolean listAllStatus;

  @CommandLine.Mixin
  private ListPaginationOptions listOptions;

  @Override
  protected OzoneAddress getAddress() {
    return snapshotPath.getValue();
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) throws IOException {
    String volumeName = snapshotPath.getValue().getVolumeName();
    String bucketName = snapshotPath.getValue().getBucketName();

    Iterator<OzoneSnapshotDiff> iterator = client.getObjectStore()
        .listSnapshotDiffJobs(volumeName, bucketName, jobStatus, listAllStatus, listOptions.getStartItem());

    int counter = printAsJsonArray(iterator, listOptions.getLimit());

    if (isVerbose()) {
      System.out.printf("Found : %d snapshot diff jobs for o3://%s/ %s ", counter, volumeName, bucketName);
    }
  }
}
