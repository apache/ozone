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

package org.apache.hadoop.ozone.debug.replicas;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.hdds.scm.cli.ScmOption;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.Shell;
import picocli.CommandLine;

/**
 * Verify replicas command.
 */

@CommandLine.Command(
    name = "verify",
    description = "Run checks to verify data across replicas")
public class ReplicasVerify extends Handler {
  @CommandLine.Mixin
  private ScmOption scmOption;

  @CommandLine.Parameters(arity = "1",
      description = Shell.OZONE_URI_DESCRIPTION)
  private String uri;

  @CommandLine.Option(names = {"-o", "--output-dir"},
      description = "Destination where the directory where the generated output will be saved.",
      defaultValue = "/opt/hadoop")
  private String outputDir;

  @CommandLine.Option(names = "--checksums",
      description = "Do client side data checksum validation of all replicas.",
      // value will be true only if the "--checksums" option was specified on the CLI
      defaultValue = "false")
  private boolean doExecuteChecksums;

  @CommandLine.Option(names = "--padding",
      description = "List all keys with any missing padding, optionally limited to a volume/bucket/key URI.",
      defaultValue = "false")
  private boolean doExecutePadding;

  private Checksums checksums;
  private FindMissingPadding findMissingPadding;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) throws IOException {
    LOG.info("Verifying replicas for {}", address);
    checksums = new Checksums(client, outputDir, LOG, getConf());
    findMissingPadding = new FindMissingPadding(client, scmOption, LOG, out(), getConf());
    findCandidateKeys(client, address);
    if (doExecuteChecksums) {
      findMissingPadding.execute();
    }
  }

  @Override
  protected OzoneAddress getAddress() throws OzoneClientException {
    return new OzoneAddress(uri);
  }

  void findCandidateKeys(OzoneClient ozoneClient, OzoneAddress address) throws IOException {
    ObjectStore objectStore = ozoneClient.getObjectStore();
    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();
    if (!keyName.isEmpty()) {
      processKey(new KeyParts(volumeName, bucketName, keyName));
    } else if (!bucketName.isEmpty()) {
      OzoneVolume volume = objectStore.getVolume(volumeName);
      OzoneBucket bucket = volume.getBucket(bucketName);
      checkBucket(bucket);
    } else if (!volumeName.isEmpty()) {
      OzoneVolume volume = objectStore.getVolume(volumeName);
      checkVolume(volume);
    } else {
      for (Iterator<? extends OzoneVolume> it = objectStore.listVolumes(null); it.hasNext();) {
        checkVolume(it.next());
      }
    }
  }

  void checkVolume(OzoneVolume volume) throws IOException {
    for (Iterator<? extends OzoneBucket> it = volume.listBuckets(null); it.hasNext();) {
      OzoneBucket bucket = it.next();
      checkBucket(bucket);
    }
  }

  void checkBucket(OzoneBucket bucket) throws IOException {
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    for (Iterator<? extends OzoneKey> it = bucket.listKeys(null); it.hasNext();) {
      OzoneKey key = it.next();
//    TODO: Remove this check once HDDS-12094 is fixed
      if (key.getName().endsWith("/")) {
        continue;
      }
      processKey(new KeyParts(volumeName, bucketName, key.getName()));
    }
  }

  void processKey(KeyParts keyParts) {
    if (doExecuteChecksums) {
      checksums.processKeyConsumer(keyParts);
    }

    if (doExecutePadding) {
      findMissingPadding.checkKeyConsumer(keyParts);
    }
  }
}
