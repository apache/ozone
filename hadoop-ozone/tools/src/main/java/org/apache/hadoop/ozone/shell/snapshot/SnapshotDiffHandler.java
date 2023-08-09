/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.shell.snapshot;

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.bucket.BucketUri;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintStream;

/**
 * ozone sh snapshot diff.
 */
@CommandLine.Command(name = "diff", aliases = "snapshotDiff",
    description = "Get the differences between two snapshots")
public class SnapshotDiffHandler extends Handler {

  @CommandLine.Mixin
  private BucketUri snapshotPath;

  @CommandLine.Parameters(description = "from snapshot name",
      index = "1")
  private String fromSnapshot;

  @CommandLine.Parameters(description = "to snapshot name",
      index = "2")
  private String toSnapshot;

  @CommandLine.Option(names = {"-t", "--token"},
      description = "continuation token for next page (optional)")
  private String token;

  @CommandLine.Option(names = {"-p", "--page-size"},
      description = "number of diff entries to be returned in the response" +
          " (optional)")
  private int pageSize;

  @CommandLine.Option(names = {"--ffd", "--force-full-diff"},
      description = "perform full diff of snapshot without using" +
          " optimised DAG based pruning approach (optional)",
      hidden = true)
  private boolean forceFullDiff;


  @CommandLine.Option(names = {"-c", "--cancel"},
      description = "Request to cancel a running SnapshotDiff job. " +
          "If the job is not IN_PROGRESS, the request will fail.",
      defaultValue = "false")
  private boolean cancel;

  @CommandLine.Option(names = {"--dnld", "--disable-native-libs-diff"},
      description = "perform diff of snapshot without using" +
          " native libs (optional)",
      hidden = true)
  private boolean diffDisableNativeLibs;

  @Override
  protected OzoneAddress getAddress() {
    return snapshotPath.getValue();
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    String volumeName = snapshotPath.getValue().getVolumeName();
    String bucketName = snapshotPath.getValue().getBucketName();
    OmUtils.validateSnapshotName(fromSnapshot);
    OmUtils.validateSnapshotName(toSnapshot);

    if (cancel) {
      cancelSnapshotDiff(client.getObjectStore(), volumeName, bucketName);
    } else {
      getSnapshotDiff(client.getObjectStore(), volumeName, bucketName);
    }
  }

  private void getSnapshotDiff(ObjectStore store, String volumeName,
                               String bucketName) throws IOException {
    try (PrintStream stream = out()) {
      stream.println(store.snapshotDiff(volumeName, bucketName, fromSnapshot,
          toSnapshot, token, pageSize, forceFullDiff, diffDisableNativeLibs));
    }
  }

  private void cancelSnapshotDiff(ObjectStore store, String volumeName,
                                  String bucketName) throws IOException {
    try (PrintStream stream = out()) {
      stream.print(store.cancelSnapshotDiff(volumeName, bucketName,
          fromSnapshot, toSnapshot));
    }
  }
}
