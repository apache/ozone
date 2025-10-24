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

package org.apache.hadoop.ozone.freon;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;


/**
 * A Freon test that creates a specified number of snapshots for a given bucket.
 * After each snapshot is created, it creates a specific number of empty files and directories.
 */
@Command(name = "snapshot-generator",
    description = "Generate snapshots and then create files and directories in the specified bucket",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
@MetaInfServices(FreonSubcommand.class)
public class SnapshotGenerator extends BaseFreonGenerator implements Callable<Void> {
  @CommandLine.ParentCommand
  private Freon freon;

  @Option(names = {"--volume"}, description = "Volume name", defaultValue = "vol1")
  private String volumeName;

  @Option(names = {"--bucket"}, description = "Bucket name", defaultValue = "buck1")
  private String bucketName;

  @Option(names = {"--snapshots"},
      description = "Number of snapshots to generate", defaultValue = "5")
  private int numSnapshots;

  @Option(names = {"--files"},
      description = "Number of files to create after each snapshot", defaultValue = "10")
  private int numFiles;

  @Option(names = {"--dirs"},
      description = "Number of directories to create after each snapshot", defaultValue = "5")
  private int numDirs;

  private OzoneConfiguration configuration;

  public SnapshotGenerator() {
  }

  @VisibleForTesting
  SnapshotGenerator(OzoneConfiguration ozoneConfiguration) {
    this.configuration = ozoneConfiguration;
  }

  @Override
  public Void call() throws Exception {
    init();
    if (configuration == null) {
      configuration = freon.getOzoneConf();
    }
    try (OzoneClient client = createOzoneClient(null, this.configuration)) {
      // Ensure that the volume and bucket exist.
      ensureVolumeAndBucketExist(client, volumeName, bucketName);
      ObjectStore store = client.getObjectStore();
      OzoneVolume volume = store.getVolume(volumeName);
      OzoneBucket bucket = volume.getBucket(bucketName);

      // Loop to create the desired number of snapshots.
      for (int snapIndex = 1; snapIndex <= numSnapshots; snapIndex++) {
        String snapshotName = String.format("snap-%d", snapIndex);

        // Create snapshot.
        String snapshotResponse = store.createSnapshot(volumeName, bucketName, snapshotName);
        System.out.println("Created snapshot: " + snapshotResponse);

        // After creating a snapshot, create the specified number of files.
        for (int fileIndex = 1; fileIndex <= numFiles; fileIndex++) {
          // Generate a unique key name.
          String keyName = generateObjectName(fileIndex + (snapIndex - 1L) * numFiles);
          // Create an empty file (a key with length 0).
          bucket.createKey(keyName, 0).close();
        }

        // After creating files, create the specified number of directories.
        // Directories are simulated as keys that end with "/" (assuming that is how directories are handled in Ozone).
        for (int dirIndex = 1; dirIndex <= numDirs; dirIndex++) {
          String dirName = String.format("dir-%d", dirIndex);
          // Append "/" to denote a directory.
          bucket.createDirectory(dirName);
        }
      }
    }
    return null;
  }
}
