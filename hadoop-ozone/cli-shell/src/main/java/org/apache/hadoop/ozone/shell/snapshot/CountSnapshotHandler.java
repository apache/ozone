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
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.snapshot.SnapshotBucketCount;
import org.apache.hadoop.ozone.snapshot.SnapshotCountResponse;
import picocli.CommandLine;

/**
 * ozone sh snapshot count.
 * Displays bucket-wise snapshot distribution in the cluster.
 */
@CommandLine.Command(name = "count",
    description = "Display bucket-wise snapshot distribution for the cluster.")
public class CountSnapshotHandler extends Handler {

  @CommandLine.Option(names = {"-b", "--bucket"},
      description = "Optional bucket filter. Accepts either <bucket> or <volume>/<bucket>.")
  private String bucketFilter;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {
    SnapshotCountResponse response = client.getObjectStore().snapshotCount(bucketFilter);
    printObjectAsJson(new ShellSnapshotCountResponse(response));
  }

  private static final class ShellSnapshotCountResponse {
    private final Count total;
    private final java.util.Map<String, Count> buckets = new java.util.TreeMap<>();

    private ShellSnapshotCountResponse(SnapshotCountResponse response) {
      this.total = new Count(response.getActive(), response.getDeleted(), response.getTotal());
      for (SnapshotBucketCount bucketCount : response.getBuckets()) {
        buckets.put(bucketCount.getVolumeName() + "/" + bucketCount.getBucketName(),
            new Count(bucketCount.getActive(), bucketCount.getDeleted(), bucketCount.getTotal()));
      }
    }

    public Count getTotal() {
      return total;
    }

    public java.util.Map<String, Count> getBuckets() {
      return buckets;
    }
  }

  private static final class Count {
    private final long active;
    private final long deleted;
    private final long total;

    private Count(long active, long deleted, long total) {
      this.active = active;
      this.deleted = deleted;
      this.total = total;
    }

    public long getActive() {
      return active;
    }

    public long getDeleted() {
      return deleted;
    }

    public long getTotal() {
      return total;
    }
  }
}
