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
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.bucket.BucketUri;
import picocli.CommandLine;

/**
 * ozone sh snapshot rename.
 */
@CommandLine.Command(name = "rename",
    description = "Rename a snapshot")
public class RenameSnapshotHandler extends Handler {

  @CommandLine.Mixin
  private BucketUri snapshotPath;

  @CommandLine.Parameters(description = "Current snapshot name",
      index = "1", arity = "1")
  private String snapshotOldName;

  @CommandLine.Parameters(description = "New snapshot name",
      index = "2", arity = "1")
  private String snapshotNewName;

  @Override
  protected OzoneAddress getAddress() {
    return snapshotPath.getValue();
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) throws IOException, OzoneClientException {
    String volumeName = snapshotPath.getValue().getVolumeName();
    String bucketName = snapshotPath.getValue().getBucketName();
    OmUtils.validateSnapshotName(snapshotNewName);
    client.getObjectStore()
        .renameSnapshot(volumeName, bucketName, snapshotOldName, snapshotNewName);
    if (isVerbose()) {
      out().format("Renamed snapshot from'%s' to %s under '%s/%s'.%n",
          snapshotOldName, snapshotNewName, volumeName, bucketName);
    }
  }
}
