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

package org.apache.hadoop.ozone.web.ozShell.keys;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.OzoneAddress;
import org.apache.hadoop.ozone.web.ozShell.Shell;

import org.apache.commons.codec.digest.DigestUtils;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Gets an existing key.
 */
@Command(name = "get",
    description = "Gets a specific key from ozone server")
public class GetKeyHandler extends Handler {

  @Parameters(index = "0", arity = "1..1", description =
      Shell.OZONE_KEY_URI_DESCRIPTION)
  private String uri;

  @Parameters(index = "1", arity = "1..1",
      description = "File path to download the key to")
  private String fileName;

  /**
   * Executes the Client Calls.
   */
  @Override
  public Void call() throws Exception {

    OzoneAddress address = new OzoneAddress(uri);
    address.ensureKeyAddress();

    OzoneConfiguration conf = createOzoneConfiguration();

    try (OzoneClient client = address.createClient(conf)) {

      String volumeName = address.getVolumeName();
      String bucketName = address.getBucketName();
      String keyName = address.getKeyName();

      if (isVerbose()) {
        System.out.printf("Volume Name : %s%n", volumeName);
        System.out.printf("Bucket Name : %s%n", bucketName);
        System.out.printf("Key Name : %s%n", keyName);
      }

      File dataFile = new File(fileName);

      if (dataFile.exists() && dataFile.isDirectory()) {
        dataFile = new File(fileName, keyName);
      }

      if (dataFile.exists()) {
        throw new OzoneClientException(dataFile.getPath() + " exists."
            + " Download would overwrite an existing file. Aborting.");
      }

      int chunkSize = (int) conf.getStorageSize(OZONE_SCM_CHUNK_SIZE_KEY,
          OZONE_SCM_CHUNK_SIZE_DEFAULT, StorageUnit.BYTES);

      OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
      OzoneBucket bucket = vol.getBucket(bucketName);
      try (OutputStream output = new FileOutputStream(dataFile);
           InputStream input = bucket.readKey(keyName)) {
        IOUtils.copyBytes(input, output, chunkSize);
      }

      if (isVerbose()) {
        try (InputStream stream = new FileInputStream(dataFile)) {
          String hash = DigestUtils.md5Hex(stream);
          System.out.printf("Downloaded file hash : %s%n", hash);
        }
      }
    }
    return null;
  }
}
