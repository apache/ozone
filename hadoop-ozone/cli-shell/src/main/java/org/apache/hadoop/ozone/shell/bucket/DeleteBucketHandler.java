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

package org.apache.hadoop.ozone.shell.bucket;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hdds.scm.net.NetConstants.PATH_SEPARATOR_STR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Delete bucket Handler.
 */
@Command(name = "delete",
    description = "deletes a bucket")
public class DeleteBucketHandler extends BucketHandler {

  @CommandLine.Option(
      names = {"-r"},
      description = "Delete bucket recursively"
  )
  private boolean bRecursive;
  @CommandLine.Option(names = {"-y", "--yes"},
      description = "Continue without interactive user confirmation")
  private boolean yes;
  private String omServiceId;
  private static final int MAX_KEY_DELETE_BATCH_SIZE = 1000;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    omServiceId = address.getOmServiceId(getConf());

    if (bRecursive) {
      if (!yes) {
        // Ask for user confirmation
        out().print("This command will delete bucket recursively." +
            "\nThere is no recovery option after using this command, " +
            "and deleted keys won't move to trash." +
            "\nEnter 'yes' to proceed': ");
        out().flush();
        Scanner scanner = new Scanner(new InputStreamReader(
            System.in, StandardCharsets.UTF_8));
        String confirmation = scanner.next().trim().toLowerCase();
        if (!confirmation.equals("yes")) {
          out().println("Operation cancelled.");
          return;
        }
      }
      OzoneBucket bucket = vol.getBucket(bucketName);
      if (bucket.getBucketLayout().equals(OBJECT_STORE)) {
        deleteOBSBucketRecursive(vol, bucket);
      } else {
        deleteFSBucketRecursive(vol, bucket);
      }
      return;
    }
    // Delete bucket without recursive
    vol.deleteBucket(bucketName);
    out().printf("Bucket %s is deleted%n", bucketName);
  }

  /**
   * Delete OBS bucket recursively.
   *
   * @param bucket OzoneBucket
   */
  private void deleteOBSBucketRecursive(OzoneVolume vol, OzoneBucket bucket) {
    ArrayList<String> keys = new ArrayList<>();
    try {
      if (!bucket.isLink()) {
        Iterator<? extends OzoneKey> iterator = bucket.listKeys(null);
        while (iterator.hasNext()) {
          keys.add(iterator.next().getName());
          if (MAX_KEY_DELETE_BATCH_SIZE == keys.size()) {
            bucket.deleteKeys(keys);
            keys.clear();
          }
        }
        // delete if any remaining keys left
        if (!keys.isEmpty()) {
          bucket.deleteKeys(keys);
        }
      }
      vol.deleteBucket(bucket.getName());
      out().printf("Bucket %s is deleted%n", bucket.getName());
    } catch (Exception e) {
      out().printf("Could not delete bucket %s.%n", e.getMessage());
    }
  }

  /**
   * Delete Legacy/FSO bucket recursively.
   *
   * @param vol String
   * @param bucket OzoneBucket
   */
  private void deleteFSBucketRecursive(OzoneVolume vol, OzoneBucket bucket) {
    try {
      String hostPrefix = OZONE_OFS_URI_SCHEME + "://" +
          omServiceId + PATH_SEPARATOR_STR;
      String ofsPrefix = hostPrefix + vol.getName() + PATH_SEPARATOR_STR +
          bucket.getName();
      final Path path = new Path(ofsPrefix);
      OzoneConfiguration clientConf = new OzoneConfiguration(getConf());
      clientConf.set(FS_DEFAULT_NAME_KEY, hostPrefix);
      FileSystem fs = FileSystem.get(clientConf);
      if (!fs.delete(path, true)) {
        out().printf("Could not delete bucket %s.%n", bucket.getName());
        return;
      }
      out().printf("Bucket %s is deleted%n", bucket.getName());
    } catch (IOException e) {
      out().printf("Could not delete bucket %s.%n", e.getMessage());
    }
  }
}
