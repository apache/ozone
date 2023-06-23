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

package org.apache.hadoop.ozone.shell.bucket;

import com.google.common.base.Strings;
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

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Scanner;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hdds.scm.net.NetConstants.PATH_SEPARATOR_STR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.OBJECT_STORE;

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

  @CommandLine.Option(
      names = {"-id", "--om-service-id"},
      description = "Ozone Manager Service ID"
  )
  private String omServiceId;

  @CommandLine.Option(names = {"-y", "--yes"},
      description = "Continue without interactive user confirmation")
  private boolean yes;

  private static final int MAX_KEY_DELETE_BATCH_SIZE = 1000;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);

    if (bRecursive) {
      Collection<String> serviceIds = getConf().getTrimmedStringCollection(
          OZONE_OM_SERVICE_IDS_KEY);
      if (Strings.isNullOrEmpty(omServiceId)) {
        if (serviceIds.size() > 1) {
          out().printf("OmServiceID not provided, provide using " +
              "-id <OM_SERVICE_ID>%n");
          return;
        } else if (serviceIds.size() == 1) {
          // Only one OM service ID configured, we can use that
          omServiceId = serviceIds.iterator().next();
        }
      }
      if (!yes) {
        // Ask for user confirmation
        out().print("This command will delete bucket recursively." +
            "\nThere is no recovery option after using this command, " +
            "and no trash for FSO buckets." +
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
      out().printf("Bucket %s is deleted%n", bucketName);
      return;
    }
    out().printf("Bucket %s is deleted%n", bucketName);
    vol.deleteBucket(bucketName);
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
        if (keys.size() > 0) {
          bucket.deleteKeys(keys);
        }
      }
      vol.deleteBucket(bucket.getName());
    } catch (Exception e) {
      out().println("Could not delete bucket.");
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
      String hostPrefix = OZONE_OFS_URI_SCHEME + "://";
      if (!Strings.isNullOrEmpty(omServiceId)) {
        hostPrefix += omServiceId + PATH_SEPARATOR_STR;
      } else {
        hostPrefix += getConf().get(OZONE_OM_ADDRESS_KEY) +
            PATH_SEPARATOR_STR;
      }
      String ofsPrefix = hostPrefix + vol.getName() + PATH_SEPARATOR_STR +
          bucket.getName();
      final Path path = new Path(ofsPrefix);
      OzoneConfiguration clientConf = new OzoneConfiguration(getConf());
      clientConf.set(FS_DEFAULT_NAME_KEY, hostPrefix);
      FileSystem fs = FileSystem.get(clientConf);
      if (!fs.delete(path, true)) {
        out().println("Could not delete bucket.");
      }
    } catch (IOException e) {
      out().println("Could not delete bucket.");
    }
  }
}
