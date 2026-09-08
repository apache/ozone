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

package org.apache.hadoop.ozone.admin.om;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.om.helpers.BucketDeletedBytes;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

/**
 * Handler of ozone admin om bucket-deleted-bytes command.
 */
@CommandLine.Command(
    name = "bucket-deleted-bytes",
    aliases = {"bdb"},
    description = "Shows snapshot-trapped vs purgeable deleted bytes for a bucket.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class BucketDeletedBytesSubCommand implements Callable<Void> {

  @CommandLine.Mixin
  private OmAddressOptions.OptionalServiceIdOrHostMixin omAddressOptions;

  @CommandLine.Parameters(index = "0", arity = "1",
      description = "Bucket path in volume/bucket format")
  private String bucketPath;

  @CommandLine.Option(names = { "--json" }, defaultValue = "false",
      description = "Print output in JSON format")
  private boolean json;

  @Override
  public Void call() throws Exception {
    try (OzoneManagerProtocol omClient = omAddressOptions.newClient()) {
      BucketDeletedBytes result = omClient.getBucketDeletedBytes(bucketPath);
      if (json) {
        printJson(result);
      } else {
        printText(result);
      }
    }
    return null;
  }

  private void printText(BucketDeletedBytes result) {
    long totalDeletedBytes =
        result.getSnapshotTrappedBytes() + result.getPurgeableBytes();
    long totalDeletedKeys =
        result.getSnapshotTrappedKeys() + result.getPurgeableKeys();
    long totalDeletedDirs =
        result.getSnapshotTrappedDirs() + result.getPurgeableDirs();
    System.out.println("Bucket: " + bucketPath);
    System.out.println("SnapshotTrappedBytes: " + result.getSnapshotTrappedBytes());
    System.out.println("PurgeableBytes: " + result.getPurgeableBytes());
    System.out.println("SnapshotTrappedKeys: " + result.getSnapshotTrappedKeys());
    System.out.println("PurgeableKeys: " + result.getPurgeableKeys());
    System.out.println("SnapshotTrappedDirs: " + result.getSnapshotTrappedDirs());
    System.out.println("PurgeableDirs: " + result.getPurgeableDirs());
    System.out.println("TotalDeletedBytes: " + totalDeletedBytes);
    System.out.println("TotalDeletedKeys: " + totalDeletedKeys);
    System.out.println("TotalDeletedDirs: " + totalDeletedDirs);
  }

  private void printJson(BucketDeletedBytes result) throws Exception {
    Map<String, Object> output = new LinkedHashMap<>();
    output.put("bucketPath", bucketPath);
    output.put("snapshotTrappedBytes", result.getSnapshotTrappedBytes());
    output.put("purgeableBytes", result.getPurgeableBytes());
    output.put("snapshotTrappedKeys", result.getSnapshotTrappedKeys());
    output.put("purgeableKeys", result.getPurgeableKeys());
    output.put("snapshotTrappedDirs", result.getSnapshotTrappedDirs());
    output.put("purgeableDirs", result.getPurgeableDirs());
    output.put("totalDeletedBytes",
        result.getSnapshotTrappedBytes() + result.getPurgeableBytes());
    output.put("totalDeletedKeys",
        result.getSnapshotTrappedKeys() + result.getPurgeableKeys());
    output.put("totalDeletedDirs",
        result.getSnapshotTrappedDirs() + result.getPurgeableDirs());
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(output));
  }
}

