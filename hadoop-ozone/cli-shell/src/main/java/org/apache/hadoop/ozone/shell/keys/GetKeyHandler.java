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

package org.apache.hadoop.ozone.shell.keys;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Gets an existing key.
 */
@Command(name = "get",
    description = "Gets a specific key from ozone server")
public class GetKeyHandler extends KeyHandler {

  @Parameters(index = "1", arity = "1..1",
      description = "File path to download the key to")
  private String fileName;

  @CommandLine.Option(
      names = {"-f", "--force"},
      description = "Overwrite local file if it exists",
      defaultValue = "false"
  )
  private boolean force;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();

    File dataFile = new File(fileName);

    if (dataFile.exists() && dataFile.isDirectory()) {
      dataFile = new File(fileName, keyName);
    }

    if (dataFile.exists() && !force) {
      throw new OzoneClientException(dataFile.getPath() + " exists."
          + " Download would overwrite an existing file. Aborting.");
    }

    int chunkSize = (int) getConf().getStorageSize(OZONE_SCM_CHUNK_SIZE_KEY,
        OZONE_SCM_CHUNK_SIZE_DEFAULT, StorageUnit.BYTES);

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);
    try (InputStream input = bucket.readKey(keyName);
        OutputStream output = Files.newOutputStream(dataFile.toPath())) {
      IOUtils.copyBytes(input, output, chunkSize);
    }

    if (isVerbose() && !"/dev/null".equals(dataFile.getAbsolutePath())) {
      try (InputStream stream = Files.newInputStream(dataFile.toPath())) {
        String hash = DigestUtils.sha256Hex(stream);
        out().printf("Downloaded file sha256 checksum : %s%n", hash);
      }
    }
  }
}
