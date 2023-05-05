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

package org.apache.hadoop.ozone.shell.keys;

import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.security.UserGroupInformation;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.IOException;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

/**
 * Executes Delete Key.
 */
@Command(name = "delete",
    description = "deletes an existing key")
public class DeleteKeyHandler extends KeyHandler {

  @CommandLine.Option(names = "--skipTrash",
          description = "Specify whether to skip Trash ")
  private boolean skipTrash = false;

  private static final Path CURRENT = new Path("Current");

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException, OzoneClientException {

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);

    float hadoopTrashInterval = getConf().getFloat(
        FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT);

    long trashInterval =
            (long) (getConf().getFloat(
                    OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY,
                    hadoopTrashInterval) * 10000);

    // If Bucket layout is FSO and Trash is enabled
    // In this case during delete operation move key to trash
    if (bucket.getBucketLayout().isFileSystemOptimized() && trashInterval > 0
        && !skipTrash && !keyName.contains(".Trash")) {

      keyName = OzoneFSUtils.removeTrailingSlashIfNeeded(keyName);
      try {
        // Check if key exists in Ozone
        OzoneKeyDetails key = bucket.getKey(keyName);
        if (key == null) {
          out().printf("Key not found %s", keyName);
          return;
        }
        // Check whether directory is empty or not
        Iterator<? extends OzoneKey> ozoneKeyIterator =
            bucket.listKeys(keyName, keyName);
        int count = 0;
        while (ozoneKeyIterator.hasNext()) {
          ozoneKeyIterator.next();
          if (++count > 1) {
            // Assume FSO Tree: /a/b1/c1/k1.txt
            // And we are trying to delete key /a/b1/c1
            // In this case count is 2  which is greater than 1
            // /a/b1/c1/ and /a/b1/c1/k1.txt
            out().printf("Directory is not empty");
            return;
          }
        }
      } catch (Exception e) {
        out().printf("Key not found %s", keyName);
        return;
      }

      final String username =
              UserGroupInformation.getCurrentUser().getShortUserName();
      Path trashRoot = new Path(OZONE_URI_DELIMITER, TRASH_PREFIX);
      Path userTrash = new Path(trashRoot, username);
      Path userTrashCurrent = new Path(userTrash, CURRENT);

      String trashDirectory = (keyName.contains("/")
          ? new Path(userTrashCurrent, keyName.substring(0,
          keyName.lastIndexOf("/")))
          : userTrashCurrent).toUri().getPath();

      String toKeyName = new Path(userTrashCurrent, keyName).toUri().getPath();
      OzoneKeyDetails toKeyDetails = null;
      try {
        // check whether key already exist in trash
        toKeyDetails = bucket.getKey(toKeyName);
      } catch (IOException e) {
        // Key doesn't exist inside trash.
      }

      if (toKeyDetails != null) {
        // if key(directory) already exist in trash, just delete the key
        bucket.deleteKey(keyName);
        return;
      }
      // Create directory inside trash
      bucket.createDirectory(trashDirectory);
      // Rename key to move inside trash folder
      bucket.renameKey(keyName, toKeyName);
      out().printf("Key moved inside Trash: %s", toKeyName);

    } else {
      bucket.deleteKey(keyName);
    }
  }
}
