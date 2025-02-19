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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.ShellReplicationOptions;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Puts a file into an ozone bucket.
 */
@Command(name = "put",
    description = "creates or overwrites an existing key")
public class PutKeyHandler extends KeyHandler {

  @Parameters(index = "1", arity = "1..1", description = "File to upload")
  private String fileName;

  @Option(names = "--stream")
  private boolean stream;

  @Mixin
  private ShellReplicationOptions replication;

  @Option(names = "--expectedGeneration",
      description = "Store key only if it already exists and its generation matches the value provided")
  private Long expectedGeneration;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();

    File dataFile = new File(fileName);

    if (isVerbose()) {
      try (InputStream stream = Files.newInputStream(dataFile.toPath())) {
        String hash = DigestUtils.sha256Hex(stream);
        out().printf("File sha256 checksum : %s%n", hash);
      }
    }

    ReplicationConfig replicationConfig =
        replication.fromParamsOrConfig(getConf());

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);

    Map<String, String> keyMetadata = new HashMap<>();
    String gdprEnabled = bucket.getMetadata().get(OzoneConsts.GDPR_FLAG);
    if (Boolean.parseBoolean(gdprEnabled)) {
      keyMetadata.put(OzoneConsts.GDPR_FLAG, Boolean.TRUE.toString());
    }

    int chunkSize = (int) getConf().getStorageSize(OZONE_SCM_CHUNK_SIZE_KEY,
        OZONE_SCM_CHUNK_SIZE_DEFAULT, StorageUnit.BYTES);

    if (stream) {
      stream(dataFile, bucket, keyName, keyMetadata,
          replicationConfig, chunkSize);
    } else {
      async(dataFile, bucket, keyName, keyMetadata,
          replicationConfig, chunkSize);
    }
  }

  private void async(
      File dataFile, OzoneBucket bucket,
      String keyName, Map<String, String> keyMetadata,
      ReplicationConfig replicationConfig, int chunkSize)
      throws IOException {
    if (isVerbose()) {
      out().println("API: async");
    }
    try (InputStream input = Files.newInputStream(dataFile.toPath());
         OutputStream output = createOrReplaceKey(bucket, keyName, dataFile.length(), keyMetadata, replicationConfig)) {
      IOUtils.copyBytes(input, output, chunkSize);
    }
  }

  private OzoneOutputStream createOrReplaceKey(OzoneBucket bucket, String keyName,
      long size, Map<String, String> keyMetadata, ReplicationConfig replicationConfig
  ) throws IOException {
    if (expectedGeneration != null) {
      final long existingGeneration = expectedGeneration;
      Preconditions.checkArgument(existingGeneration > 0,
          "expectedGeneration must be positive, but was %s", existingGeneration);
      return bucket.rewriteKey(keyName, size, existingGeneration, replicationConfig, keyMetadata);
    }

    return bucket.createKey(keyName, size, replicationConfig, keyMetadata);
  }

  private void stream(
      File dataFile, OzoneBucket bucket,
      String keyName, Map<String, String> keyMetadata,
      ReplicationConfig replicationConfig, int chunkSize)
      throws IOException {
    if (isVerbose()) {
      out().println("API: streaming");
    }
    // In streaming mode, always resolve replication config at client side,
    // because streaming is not compatible for writing EC keys.
    replicationConfig = ReplicationConfig.resolve(replicationConfig,
        bucket.getReplicationConfig(), getConf());
    Preconditions.checkArgument(
        !(replicationConfig instanceof ECReplicationConfig),
        "Can not put EC key by streaming");

    try (RandomAccessFile raf = new RandomAccessFile(dataFile, "r");
         OzoneDataStreamOutput out = bucket.createStreamKey(keyName,
             dataFile.length(), replicationConfig, keyMetadata)) {
      FileChannel ch = raf.getChannel();
      long len = raf.length();
      long off = 0;
      while (len > 0) {
        long writeLen = Math.min(len, chunkSize);
        ByteBuffer bb = ch.map(FileChannel.MapMode.READ_ONLY, off, writeLen);
        out.write(bb);
        off += writeLen;
        len -= writeLen;
      }
    }
  }
}
